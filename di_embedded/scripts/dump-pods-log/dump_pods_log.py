import argparse
import zipfile
import shutil
import os
from kubernetes import client,config
def expose_pods_log(corev1_api, namespace, previous=False):
    pods = corev1_api.list_namespaced_pod(namespace=namespace)
    save_log_dir = "./logs/workloadpods/{}".format(namespace)
    if previous:
        save_log_dir +='_previous'
    if not os.path.exists(save_log_dir):
        os.makedirs(save_log_dir)
    for pod in pods.items:
        try:
            for container in pod.spec.containers:                  
                logs=corev1_api.read_namespaced_pod_log(name=pod.metadata.name,namespace=pod.metadata.namespace, container=container.name, pretty=True, previous=previous)
                with open("{}/{}-{}.log".format(save_log_dir, pod.metadata.name, container.name), "w", encoding="utf-8") as f:
                    f.writelines(logs)
        except Exception as e:
            print("Dump pod {} log error, error message:\n {}".format(pod.metadata.name, str(e)))
def archive_logs(stage):
    ziped_file='./logs/workloadPodslog_' + stage + '.zip' if stage else './logs/workloadPodslog.zip'
    log_file_dir = "./logs/workloadpods"
    z = zipfile.ZipFile(ziped_file,'w',zipfile.ZIP_DEFLATED) 
    for dirpath, dirnames, filenames in os.walk(log_file_dir):
        fpath = dirpath.replace(log_file_dir,'') 
        fpath = fpath and fpath + os.sep or ''
        for filename in filenames:
            z.write(os.path.join(dirpath, filename),fpath+filename)
    z.close()
    shutil.rmtree(log_file_dir, True)

def main(margs):
    config.load_kube_config(margs.kubeconfig)
    cust_api = client.CustomObjectsApi()
    corev1_api = client.CoreV1Api()
    for namespace in margs.namespaces:
        expose_pods_log(corev1_api, namespace)
        expose_pods_log(corev1_api, namespace, previous=True)
    stage = margs.stage if 'stage' in margs else None
    archive_logs(stage)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ClusterGroup Cleaner Robot")
    parser.add_argument("--kubeconfig", default=os.getenv("KUBECONFIG"))
    parser.add_argument(
        "--namespaces",
        nargs="+",
        help="Pods in these namespace that need to be dumped",
        required=True,
    )
    parser.add_argument("--stage")
    args = parser.parse_args()
    main(args)
