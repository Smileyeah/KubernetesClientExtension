using ICSharpCode.SharpZipLib.GZip;
using ICSharpCode.SharpZipLib.Tar;
using k8s;
using k8s.Models;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ApiServerSdkAccess
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var config = KubernetesClientConfiguration.BuildConfigFromConfigFile(new FileInfo("config/cls-8ok9q7cu-config"));
            //var config = new KubernetesClientConfiguration { Host = "http://192.168.0.168:8081" };
            IKubernetes client = new Kubernetes(config);
            Console.WriteLine("Starting Request!");

            var list = client.ListNamespacedStatefulSet("default");

            // 创建pod和service
            //var result = await CreateStatefulSetWithVolumeTemplate(
            //    client,
            //    "sdk-create-test",
            //    "sdk-create-test-container",
            //    "bounding-sc").ConfigureAwait(false);

            //var service = await CreateService(
            //    client,
            //    "sdk-create-test",
            //    30010,
            //    list.Items.First(s => s.Metadata.Name == "sdk-create-test").Metadata.Labels).ConfigureAwait(false);

            //var podsMetrics = await client.GetKubernetesPodsMetricsByNamespaceAsync("default").ConfigureAwait(false);

            //var evt = client.ReadNamespacedStatefulSetStatus("iotcenter-sqlite-v2", "default");

            //Console.WriteLine(JsonConvert.SerializeObject(evt));

            //await PodsMetrics(client).ConfigureAwait(false);

            // 在linux上测试是通过的，但Windows测试不通过
            //var cpfmRes = await CopyFileFromPodAsync(client, "/opt/ganwei/runGW.sh", Path.Combine(Environment.CurrentDirectory, "runGW.sh")).ConfigureAwait(false);

            try
            {

                await CopyFileToPodAsync(
                    client,
                    Path.Combine(Environment.CurrentDirectory, "out.txt"),
                    $"/opt/ganwei/out.txt").ConfigureAwait(false);
            }
            catch (Exception)
            {
            }

            await ExecInPod(client).ConfigureAwait(false);

            PodLables(client);

            EventListen(client);
        }

        private static void EventListen(IKubernetes client)
        {
            var list = client.ListNamespacedPodWithHttpMessagesAsync("default");
            foreach (var item in list.Result.Body.Items)
            {
                Console.WriteLine(item.Metadata.Name);
            }

            using (list.Watch<V1Pod, V1PodList>((type, item) =>
            {
                Console.WriteLine("==on watch event==");
                Console.WriteLine(type);
                Console.WriteLine(item.Metadata.Name);
                Console.WriteLine("==on watch event==");
            }))
            {
                Console.WriteLine("press ctrl + c to stop watching");

                var ctrlc = new ManualResetEventSlim(false);
                Console.CancelKeyPress += (sender, eventArgs) => ctrlc.Set();
                ctrlc.Wait();
            }
        }

        private static async Task PodsMetrics(IKubernetes client)
        {
            var podsMetrics = await client.GetKubernetesPodsMetricsByNamespaceAsync("default").ConfigureAwait(false);

            if (!podsMetrics.Items.Any())
            {
                Console.WriteLine("Empty");
            }

            foreach (var item in podsMetrics.Items)
            {
                foreach (var container in item.Containers)
                {
                    Console.WriteLine(container.Name);

                    foreach (var metric in container.Usage)
                    {
                        Console.WriteLine($"{metric.Key}: {metric.Value.CanonicalizeString()}");
                    }
                }

                Console.Write(Environment.NewLine);
            }
        }

        private static async Task NodesMetrics(IKubernetes client)
        {
            var nodesMetrics = await client.GetKubernetesNodesMetricsAsync().ConfigureAwait(false);

            foreach (var item in nodesMetrics.Items)
            {
                Console.WriteLine(item.Metadata.Name);

                foreach (var metric in item.Usage)
                {
                    Console.WriteLine($"{metric.Key}: {metric.Value}");
                }
            }
        }

        private static void PodLables(IKubernetes client)
        {
            var list = client.ListNamespacedService("default");
            foreach (var item in list.Items)
            {
                Console.WriteLine("Pods for service: " + item.Metadata.Name);
                Console.WriteLine("Type of service: " + item.Spec.Type);
                Console.WriteLine("ClusterIP of service: " + item.Spec.ClusterIP);
                Console.WriteLine("NodePort of service: " + item.Spec.Ports[0].NodePort);
                Console.WriteLine("Port of service: " + item.Spec.Ports[0].Port);
                Console.WriteLine("=-=-=-=-=-=-=-=-=-=-=");
                if (item.Spec == null || item.Spec.Selector == null)
                {
                    continue;
                }

                var labels = new List<string>();
                foreach (var key in item.Spec.Selector)
                {
                    labels.Add(key.Key + "=" + key.Value);
                }
                //PatchNamespacedStatefulSetScale
                var labelStr = string.Join(",", labels.ToArray());
                Console.WriteLine(labelStr);
                var podList = client.ListNamespacedPod("default", labelSelector: labelStr);
                foreach (var pod in podList.Items)
                {
                    Console.WriteLine(pod.Metadata.Name);
                }

                if (podList.Items.Count == 0)
                {
                    Console.WriteLine("Empty!");
                }

                Console.WriteLine();
            }
        }

        private static async Task CreateNameSpaceService(IKubernetes client)
        {
            await client.CreateNamespacedServiceAsync(new V1Service()
            {
                Spec = new V1ServiceSpec()
                {
                    Ports = new[] {
                        new V1ServicePort() {
                            Name="",
                            NodePort=30001,
                            Port=44380,
                            Protocol ="TCP",
                            TargetPort =new IntstrIntOrString("44380")
                        }
                    },
                    Type = "NodePort"
                }
            }, "default");
        }

        private async static Task ExecInPod(IKubernetes client)
        {
            var list = client.ListNamespacedPod("default");
            var pod = list.Items[1];

            //using var webSocket =
            //   await client.WebSocketNamespacedPodExecAsync(
            //       pod.Metadata.Name,
            //       "default",
            //       new string[] { "/bin/sh", "-c", "ls -R | grep \"^d\" | awk '{print i$0}' i=`pwd`'/'" },
            //       pod.Spec.Containers[0].Name).ConfigureAwait(false);

            //using var demux = new StreamDemuxer(webSocket);
            //demux.Start();

            //using StreamReader stdout = new StreamReader(demux.GetStream(1, 1), Encoding.UTF8);

            //string line;
            //while ((line = await stdout.ReadLineAsync()) != null)
            //{
            //    Console.WriteLine(line);
            //}

            while (true)
            {
                var comArr = Console.ReadLine();

                using var webSocket =
                   await client.WebSocketNamespacedPodExecAsync(
                       pod.Metadata.Name,
                       "default",
                       new string[] { "bash", "-c", comArr },
                       pod.Spec.Containers[0].Name).ConfigureAwait(false);

                using var demux = new StreamDemuxer(webSocket);
                demux.Start();

                using StreamReader stdout = new StreamReader(demux.GetStream(1, 1), Encoding.UTF8);

                // Read from STDOUT until process terminates.
                string line;
                while ((line = await stdout.ReadLineAsync()) != null)
                {
                    Console.WriteLine(line);
                }
            }
        }

        #region K8s copy

        public static async Task<int> CopyFileFromPodAsync(IKubernetes client, string sourceFilePath, string destinationFilePath, CancellationToken cancellationToken = default)
        {
            var list = client.ListNamespacedPod("default");
            var pod = list.Items[1];

            string name = pod.Metadata.Name;
            string @namespace = "default";
            string container = pod.Spec.Containers[0].Name;
            // All other parameters are being validated by MuxedStreamNamespacedPodExecAsync called by NamespacedPodExecAsync
            if (string.IsNullOrWhiteSpace(sourceFilePath))
            {
                throw new ArgumentException($"{nameof(sourceFilePath)} cannot be null or whitespace");
            }

            if (string.IsNullOrWhiteSpace(destinationFilePath))
            {
                throw new ArgumentException($"{nameof(destinationFilePath)} cannot be null or whitespace");
            }

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

            var folderName = GetLinuxFormatFolderName(sourceFilePath);

            var sourceFileInfo = new FileInfo(sourceFilePath);

            var command = $"tar czf - -C {folderName} {sourceFileInfo.Name} | base64";

            Console.WriteLine($"command: {command}");

            return await client.NamespacedPodExecAsync(
                name,
                @namespace,
                container,
                new string[] { "sh", "-c", command },
                false,
                handler,
                cancellationToken).ConfigureAwait(false);
        }

        public static async Task CopyFileToPodAsync(IKubernetes client, string sourceFilePath, string destinationFilePath, CancellationToken cancellationToken = default)
        {
            var list = client.ListNamespacedPod("default");
            var pod = list.Items[1];

            string name = pod.Metadata.Name;
            Console.WriteLine($"Pod Name: {name}");

            string @namespace = pod.Metadata.NamespaceProperty;
            string container = pod.Spec.Containers[0].Name;

            // All other parameters are being validated by MuxedStreamNamespacedPodExecAsync called by NamespacedPodExecAsync
            if (string.IsNullOrWhiteSpace(sourceFilePath))
            {
                throw new ArgumentException($"{nameof(sourceFilePath)} cannot be null or whitespace");
            }

            if (string.IsNullOrWhiteSpace(destinationFilePath))
            {
                throw new ArgumentException($"{nameof(destinationFilePath)} cannot be null or whitespace");
            }

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
                    await outputStream.CopyToAsync(cryptoStream, cancellationToken).ConfigureAwait(false);

                }
                catch (Exception ex)
                {
                    throw new IOException($"Copy command failed: {ex.Message}");
                }

                //using var errorReader = new StreamReader(stdError);
                //if (errorReader.Peek() != -1)
                //{
                //    var error = await errorReader.ReadToEndAsync().ConfigureAwait(false);
                //    throw new IOException($"Copy command failed: {error}");
                //}
            });

            var destinationFolder = GetLinuxFormatFolderName(destinationFilePath);

            using (var muxedStream = await client.MuxedStreamNamespacedPodExecAsync(
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

        private static string GetLinuxFormatFolderName(string destinationFilePath)
        {
            var folderName = Path.GetDirectoryName(destinationFilePath);

            return string.IsNullOrEmpty(folderName) ? 
                "." :
                System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Windows) ?
                folderName.Replace('\\', '/') :
                folderName; ;
        }

        #endregion

        private static async Task<V1StatefulSet> CreateStatefulSetWithVolumeTemplate(
            IKubernetes client,
            string statefulNm,
            string containerNm,
            string storageClassNm,
            IDictionary<string, ResourceQuantity> Limits = null,
            IDictionary<string, ResourceQuantity> Requests = null,
            string imageNm = "ccr.ccs.tencentyun.com/tke-iotcenter/iotcenter:3.1.13.10",
            string nameSpace = "default")
        {
            var stateful = new V1StatefulSet();
            stateful.Metadata = new V1ObjectMeta();
            stateful.Metadata.Name = statefulNm;
            stateful.Metadata.NamespaceProperty = nameSpace;
            stateful.Metadata.CreationTimestamp = DateTime.UtcNow;

            stateful.Metadata.ManagedFields = new V1ManagedFieldsEntry[]
            {
                new V1ManagedFieldsEntry
                {
                    ApiVersion = "v1",
                    Manager = "kube-controller-manager",
                    Operation = "Update",
                    Time = DateTime.UtcNow
                },
                new V1ManagedFieldsEntry
                {
                    ApiVersion = "v1",
                    Manager = "tke-apiserver",
                    Operation = "Update",
                    Time = DateTime.UtcNow
                }
            };

            var lables = new Dictionary<string, string>() { { "k8s-app", statefulNm }, { "qcloud-app", statefulNm } };
            stateful.Metadata.Labels = lables;

            stateful.Spec = new V1StatefulSetSpec();
            stateful.Spec.PodManagementPolicy = "OrderedReady";
            stateful.Spec.Replicas = 1;
            stateful.Spec.RevisionHistoryLimit = 10;
            stateful.Spec.Selector = new V1LabelSelector() { MatchLabels = lables };
            stateful.Spec.ServiceName = string.Empty;

            stateful.Spec.Template = new V1PodTemplateSpec();
            stateful.Spec.Template.Metadata = new V1ObjectMeta() { CreationTimestamp = null, Labels = lables };
            stateful.Spec.Template.Spec = new V1PodSpec();

            //stateful.Spec.VolumeClaimTemplates = new V1PersistentVolumeClaim[] {
            //    new V1PersistentVolumeClaim() {
            //        Metadata = new V1ObjectMeta() {
            //            Name = statefulNm
            //        },
            //        Spec = new V1PersistentVolumeClaimSpec(){
            //            AccessModes = new string[]{ "ReadWriteOnce" },
            //            StorageClassName = storageClassNm,
            //            Resources = new V1ResourceRequirements(){
            //                Requests = new Dictionary<string, ResourceQuantity> {
            //                    { "storage", new ResourceQuantity("10Gi") }
            //                }
            //            }
            //        }
            //    }
            //};

            stateful.Spec.Template.Spec.Containers = new V1Container[] {
                new V1Container() {
                    Env = new V1EnvVar[] {
                        new V1EnvVar() {
                            Name = "PATH",
                            Value = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
                        },
                        new V1EnvVar() {
                            Name = "DOTNET_RUNNING_IN_CONTAINER",
                            Value = "true"
                        },
                        new V1EnvVar() {
                            Name = "DOTNET_ROOT",
                            Value = "/usr/share/dotnet"
                        }
                    },
                    Image = imageNm,
                    ImagePullPolicy = "IfNotPresent",
                    Name = containerNm,
                    Ports = new V1ContainerPort[] {
                        new V1ContainerPort(44380)
                    },
                    Resources = new V1ResourceRequirements(){
                        Limits = Limits?? new Dictionary<string, ResourceQuantity>() {
                                { "cpu", new ResourceQuantity("4") },
                                { "memory", new ResourceQuantity("8Gi")
                            }
                        },
                        Requests = Requests?? new Dictionary<string, ResourceQuantity>() {
                                { "cpu", new ResourceQuantity("2") },
                                { "memory", new ResourceQuantity("4Gi")
                            }
                        }
                    },
                    SecurityContext = new V1SecurityContext(privileged: false),
                    TerminationMessagePath = "/dev/termination-log",
                    TerminationMessagePolicy = "File"
                }
            };

            stateful.Spec.Template.Spec.DnsPolicy = "ClusterFirst";
            stateful.Spec.Template.Spec.ImagePullSecrets = new V1LocalObjectReference[] {
                new V1LocalObjectReference("qcloudregistrykey")
            };

            stateful.Spec.Template.Spec.RestartPolicy = "Always";
            stateful.Spec.Template.Spec.SchedulerName = "default-scheduler";
            stateful.Spec.Template.Spec.SecurityContext = new V1PodSecurityContext();
            stateful.Spec.Template.Spec.TerminationGracePeriodSeconds = 30;

            stateful.Spec.UpdateStrategy = new V1StatefulSetUpdateStrategy()
            {
                RollingUpdate = new V1RollingUpdateStatefulSetStrategy(0),
                Type = "RollingUpdate"
            };

            return await client.CreateNamespacedStatefulSetAsync(
                stateful,
                nameSpace
                ).ConfigureAwait(false);
        }

        private static async Task<V1Service> CreateService(
            IKubernetes client,
            string srvName,
            int nodePort,
            IDictionary<string, string> selector,
            string nameSpace = "default")
        {
            var service = new V1Service();
            service.Metadata = new V1ObjectMeta();
            service.Metadata.Name = srvName;
            service.Metadata.NamespaceProperty = nameSpace;
            service.Metadata.CreationTimestamp = DateTime.UtcNow;
            service.Metadata.Labels = selector;
            service.Metadata.ManagedFields = new V1ManagedFieldsEntry[]
            {
                new V1ManagedFieldsEntry
                {
                    ApiVersion = "v1",
                    Manager = "tke-apiserver",
                    Operation = "Update",
                    Time = DateTime.UtcNow
                }
            };

            service.Spec = new V1ServiceSpec();
            service.Spec.ExternalTrafficPolicy = "Cluster";
            service.Spec.Ports = new V1ServicePort[]
            {
                new V1ServicePort
                {
                    Protocol = "TCP",
                    TargetPort = 44380,
                    Port = 44380,
                    NodePort = nodePort,
                    Name = $"44380-44380-{nodePort}-tcp"
                }
            };

            service.Spec.Selector = selector;
            service.Spec.SessionAffinity = "None";
            service.Spec.Type = "NodePort";

            return await client.CreateNamespacedServiceAsync(
                service,
                nameSpace
                ).ConfigureAwait(false);
        }
    }
}
