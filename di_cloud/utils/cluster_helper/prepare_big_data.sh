#!/bin/bash

#set -x
download_kubectl() {
  curl -LO https://storage.googleapis.com/kubernetes-release/release/v1.13.8/bin/linux/amd64/kubectl
  chmod +x kubectl
  cp kubectl /usr/local/bin/kubectl 
}

increase_vrep_pvc_size() {
  python ./get_kubeconfig.py
  export KUBECONFIG=./kubeconfig.yaml
  kubectl -n datahub get pvc layers-volume-vsystem-vrep-0
  kubectl -n datahub patch sc dh-default-ssd-storage-class -p '{"allowVolumeExpansion": true}' --type=merge
  kubectl -n datahub patch pvc layers-volume-vsystem-vrep-0 -p '{"spec": {"resources": {"requests": {"storage": "75Gi"}}}}' --type=merge
  kubectl -n datahub patch dh default -p '{"spec": {"vsystem": {"pvcSize": "75Gi"}}}' --type=merge
  sleep 300
  kubectl -n datahub get pvc layers-volume-vsystem-vrep-0
}

download_vctl() {
    wget -c https://int.repositories.cloud.sap/artifactory/build-releases-xmake/com/sap/hana/hl/linuxx86_64/vsystem-client/2013.26.6/vsystem-client-2013.26.6-linuxx86_64.tar.gz --no-check-certificate
    tar -xf vsystem-client-2013.26.6-linuxx86_64.tar.gz
    chmod +x ./vctl
    cp ./vctl /usr/local/bin/
    rm -f vsystem-client-*-linuxx86_64.tar.gz
}
 
write_to_vrep() {
  vctl vrep user mkdir "/big-data"
  local file_to_write="${1}"
  for i in {1..500}
  do
    echo $i
    vctl vrep user put "${file_to_write}" "/big-data/massive_data_100M_${i}"
    sleep .2
  done
}

download_kubectl
increase_vrep_pvc_size
download_vctl
vctl login ${url} default tester "Test123!"
head -c 100M </dev/urandom | base64 > randomfile
write_to_vrep randomfile