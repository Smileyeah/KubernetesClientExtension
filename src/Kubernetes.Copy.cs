using System;
using System.Collections.Generic;
using System.Text;

namespace ApiServerSdkAccess
{
    using ICSharpCode.SharpZipLib.GZip;
    using ICSharpCode.SharpZipLib.Tar;
    using k8s;
    using k8s.Models;
    using System.IO;
    using System.Security.Cryptography;
    using System.Threading;
    using System.Threading.Tasks;

    public static class KubernetesExtension
    {
        /*******************************************************************
        ** /!\ Requires that the 'tar' binary is present in your container
        ** image. If 'tar' is not present, the copy will fail. /!\
        *******************************************************************/
        public static async Task<int> CopyFileFromPodAsync(this IKubernetes kubernetes, V1Pod pod, string container, string sourceFilePath, string destinationFilePath, CancellationToken cancellationToken = default)
        {
            if (pod == null)
            {
                throw new ArgumentNullException(nameof(pod));
            }

            return await kubernetes.CopyFileFromPodAsync(pod.Metadata.Name, pod.Metadata.NamespaceProperty, container, sourceFilePath, destinationFilePath, cancellationToken).ConfigureAwait(false);
        }

        public static async Task<int> CopyFileFromPodAsync(this IKubernetes kubernetes, string name, string @namespace, string container, string sourceFilePath, string destinationFilePath, CancellationToken cancellationToken = default)
        {
            // All other parameters are being validated by MuxedStreamNamespacedPodExecAsync called by NamespacedPodExecAsync
            ValidatePathParameters(sourceFilePath, destinationFilePath);

            // The callback which processes the standard input, standard output and standard error of exec method
            var handler = new ExecAsyncCallback(async (stdIn, stdOut, stdError) =>
            {
                using (var errorReader = new StreamReader(stdError))
                {
                    if (errorReader.Peek() != -1)
                    {
                        var error = await errorReader.ReadToEndAsync().ConfigureAwait(false);
                        throw new IOException($"Copy command failed: {error}");
                    }
                }

                try
                {
                    using var stream = new CryptoStream(stdOut, new FromBase64Transform(), CryptoStreamMode.Read);
                    using var gzipStream = new GZipInputStream(stream);
                    using var tarInputStream = new TarInputStream(gzipStream, Encoding.UTF8);
                    var tarEntry = tarInputStream.GetNextEntry();
                    var directoryName = Path.GetDirectoryName(destinationFilePath);

                    if (!string.IsNullOrEmpty(directoryName))
                    {
                        Directory.CreateDirectory(directoryName);
                    }

                    using var outputFile = new FileStream(destinationFilePath, FileMode.Create);
                    tarInputStream.CopyEntryContents(outputFile);
                }
                catch (Exception ex)
                {
                    throw new IOException($"Copy command failed: {ex.Message}");
                }
            });

            var sourceFileInfo = new FileInfo(sourceFilePath);
            var sourceFolder = GetLinuxFolderName(sourceFilePath);

            return await kubernetes.NamespacedPodExecAsync(
                name,
                @namespace,
                container,
                new string[] { "sh", "-c", $"tar czf - -C {sourceFolder} {sourceFileInfo.Name} | base64" },
                false,
                handler,
                cancellationToken).ConfigureAwait(false);
        }

        public static async Task CopyFileToPodAsync(this IKubernetes kubernetes, V1Pod pod, string container, string sourceFilePath, string destinationFilePath, CancellationToken cancellationToken = default)
        {
            if (pod == null)
            {
                throw new ArgumentNullException(nameof(pod));
            }

            await kubernetes.CopyFileToPodAsync(pod.Metadata.Name, pod.Metadata.NamespaceProperty, container, sourceFilePath, destinationFilePath, cancellationToken).ConfigureAwait(false);
        }

        public static async Task CopyFileToPodAsync(this IKubernetes kubernetes, string name, string @namespace, string container, string sourceFilePath, string destinationFilePath, CancellationToken cancellationToken = default)
        {
            // All other parameters are being validated by MuxedStreamNamespacedPodExecAsync called by NamespacedPodExecAsync
            ValidatePathParameters(sourceFilePath, destinationFilePath);

            // The callback which processes the standard input, standard output and standard error of exec method
            var handler = new ExecAsyncCallback(async (stdIn, stdOut, stdError) =>
            {
                var fileInfo = new FileInfo(destinationFilePath);

                try
                {
                    using var outputStream = new MemoryStream();
                    using (var inputFileStream = File.OpenRead(sourceFilePath))
                    using (var gZipOutputStream = new GZipOutputStream(outputStream))
                    using (var tarOutputStream = new TarOutputStream(gZipOutputStream, Encoding.UTF8))
                    {
                        // To avoid gZipOutputStream to close the memoryStream
                        gZipOutputStream.IsStreamOwner = false;

                        var fileSize = inputFileStream.Length;
                        var entry = TarEntry.CreateTarEntry(fileInfo.Name);
                        entry.Size = fileSize;

                        tarOutputStream.PutNextEntry(entry);

                        // this is copied from TarArchive.WriteEntryCore
                        byte[] localBuffer = new byte[32 * 1024];
                        while (true)
                        {
                            int numRead = inputFileStream.Read(localBuffer, 0, localBuffer.Length);
                            if (numRead <= 0)
                            {
                                break;
                            }

                            tarOutputStream.Write(localBuffer, 0, numRead);
                        }

                        tarOutputStream.CloseEntry();
                    }

                    outputStream.Position = 0;
                    using var cryptoStream = new CryptoStream(stdIn, new ToBase64Transform(), CryptoStreamMode.Write);
                    await outputStream.CopyToAsync(cryptoStream).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    throw new IOException($"Copy command failed: {ex.Message}");
                }

            });

            var destinationFolder = GetLinuxFolderName(destinationFilePath);

            using (var muxedStream = await kubernetes.MuxedStreamNamespacedPodExecAsync(
                    name,
                    @namespace,
                    new string[] { "sh", "-c", $"base64 -d | tar xzmf - -C {destinationFolder}" },
                    container, tty: false,
                    cancellationToken: cancellationToken).ConfigureAwait(false))
            using (var stdIn = muxedStream.GetStream(null, ChannelIndex.StdIn))
            using (var stdOut = muxedStream.GetStream(ChannelIndex.StdOut, null))
            using (var stdErr = muxedStream.GetStream(ChannelIndex.StdErr, null))
            using (var error = muxedStream.GetStream(ChannelIndex.Error, null))
            using (var errorReader = new StreamReader(error))
            {
                muxedStream.Start();

                await handler(stdIn, stdOut, stdErr).ConfigureAwait(false);
            }
        }

        public static async Task<int> CopyDirectoryFromPodAsync(this IKubernetes kubernetes, V1Pod pod, string container, string sourceFolderPath, string destinationFolderPath, CancellationToken cancellationToken = default)
        {
            if (pod == null)
            {
                throw new ArgumentNullException(nameof(pod));
            }

            return await kubernetes.CopyDirectoryFromPodAsync(pod.Metadata.Name, pod.Metadata.NamespaceProperty, container, sourceFolderPath, destinationFolderPath, cancellationToken).ConfigureAwait(false);

        }

        public static async Task<int> CopyDirectoryFromPodAsync(this IKubernetes kubernetes, string name, string @namespace, string container, string sourceFolderPath, string destinationFolderPath, CancellationToken cancellationToken = default)
        {
            // All other parameters are being validated by MuxedStreamNamespacedPodExecAsync called by NamespacedPodExecAsync
            ValidatePathParameters(sourceFolderPath, destinationFolderPath);

            // The callback which processes the standard input, standard output and standard error of exec method
            var handler = new ExecAsyncCallback(async (stdIn, stdOut, stdError) =>
            {
                using (var errorReader = new StreamReader(stdError))
                {
                    if (errorReader.Peek() != -1)
                    {
                        var error = await errorReader.ReadToEndAsync().ConfigureAwait(false);
                        throw new IOException($"Copy command failed: {error}");
                    }
                }

                try
                {
                    using var stream = new CryptoStream(stdOut, new FromBase64Transform(), CryptoStreamMode.Read);
                    using var gzipStream = new GZipInputStream(stream);
                    using var tarArchive = TarArchive.CreateInputTarArchive(gzipStream, Encoding.UTF8);
                    tarArchive.ExtractContents(destinationFolderPath);
                }
                catch (Exception ex)
                {
                    throw new IOException($"Copy command failed: {ex.Message}");
                }
            });

            return await kubernetes.NamespacedPodExecAsync(
                name,
                @namespace,
                container,
                new string[] { "sh", "-c", $"tar czf - -C {sourceFolderPath} . | base64" },
                false,
                handler,
                cancellationToken).ConfigureAwait(false);
        }

        public static async Task CopyDirectoryToPodAsync(this IKubernetes kubernetes, V1Pod pod, string container, string sourceDirectoryPath, string destinationDirectoyPath, CancellationToken cancellationToken = default)
        {
            if (pod == null)
            {
                throw new ArgumentNullException(nameof(pod));
            }

            await kubernetes.CopyDirectoryToPodAsync(pod.Metadata.Name, pod.Metadata.NamespaceProperty, container, sourceDirectoryPath, destinationDirectoyPath, cancellationToken).ConfigureAwait(false);
        }

        public static async Task CopyDirectoryToPodAsync(this IKubernetes kubernetes, string name, string @namespace, string container, string sourceDirectoryPath, string destinationDirectoryPath, CancellationToken cancellationToken = default)
        {
            // All other parameters are being validated by MuxedStreamNamespacedPodExecAsync called by NamespacedPodExecAsync
            ValidatePathParameters(sourceDirectoryPath, destinationDirectoryPath);

            // The callback which processes the standard input, standard output and standard error of exec method
            var handler = new ExecAsyncCallback(async (stdIn, stdOut, stdError) =>
            {
                try
                {
                    using var outputStream = new MemoryStream();
                    using (var gZipOutputStream = new GZipOutputStream(outputStream))
                    using (var tarArchive = TarArchive.CreateOutputTarArchive(gZipOutputStream))
                    {
                        // To avoid gZipOutputStream to close the memoryStream
                        gZipOutputStream.IsStreamOwner = false;

                        // RootPath must be forward slashes and must not end with a slash
                        tarArchive.RootPath = sourceDirectoryPath.Replace('\\', '/');
                        if (tarArchive.RootPath.EndsWith("/", StringComparison.InvariantCulture))
                        {
                            tarArchive.RootPath = tarArchive.RootPath.Remove(tarArchive.RootPath.Length - 1);
                        }

                        AddDirectoryFilesToTar(tarArchive, sourceDirectoryPath);
                    }

                    outputStream.Position = 0;
                    using var cryptoStream = new CryptoStream(stdIn, new ToBase64Transform(), CryptoStreamMode.Write);
                    await outputStream.CopyToAsync(cryptoStream).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    throw new IOException($"Copy command failed: {ex.Message}");
                }
            });

            using (var muxedStream = await kubernetes.MuxedStreamNamespacedPodExecAsync(
                    name,
                    @namespace,
                    new string[] { "sh", "-c", $"base64 -d | tar xzmf - -C {destinationDirectoryPath}" },
                    container, tty: false,
                    cancellationToken: cancellationToken).ConfigureAwait(false))
            using (var stdIn = muxedStream.GetStream(null, ChannelIndex.StdIn))
            using (var stdOut = muxedStream.GetStream(ChannelIndex.StdOut, null))
            using (var stdErr = muxedStream.GetStream(ChannelIndex.StdErr, null))
            using (var error = muxedStream.GetStream(ChannelIndex.Error, null))
            using (var errorReader = new StreamReader(error))
            {
                muxedStream.Start();

                await handler(stdIn, stdOut, stdErr).ConfigureAwait(false);
            }
        }

        private static void AddDirectoryFilesToTar(TarArchive tarArchive, string sourceDirectoryPath)
        {
            var tarEntry = TarEntry.CreateEntryFromFile(sourceDirectoryPath);
            tarArchive.WriteEntry(tarEntry, false);

            var filenames = Directory.GetFiles(sourceDirectoryPath);
            for (var i = 0; i < filenames.Length; i++)
            {
                tarEntry = TarEntry.CreateEntryFromFile(filenames[i]);
                tarArchive.WriteEntry(tarEntry, true);
            }

            var directories = Directory.GetDirectories(sourceDirectoryPath);
            for (var i = 0; i < directories.Length; i++)
            {
                AddDirectoryFilesToTar(tarArchive, directories[i]);
            }
        }

        private static string GetLinuxFolderName(string filePath)
        {
            var folderName = Path.GetDirectoryName(filePath);

            return string.IsNullOrEmpty(folderName) ?
                "." :
                System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Windows) ?
                folderName.Replace('\\', '/') :
                folderName;
        }

        private static void ValidatePathParameters(string sourcePath, string destinationPath)
        {
            if (string.IsNullOrWhiteSpace(sourcePath))
            {
                throw new ArgumentException($"{nameof(sourcePath)} cannot be null or whitespace");
            }

            if (string.IsNullOrWhiteSpace(destinationPath))
            {
                throw new ArgumentException($"{nameof(destinationPath)} cannot be null or whitespace");
            }

        }
    }
}
