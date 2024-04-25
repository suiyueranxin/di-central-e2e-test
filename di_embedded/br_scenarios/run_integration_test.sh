#!/bin/bash

CLUSTERNAME=${1:-""}

if [ "$CLUSTERNAME" == "" ]; then
  echo "Missing region name !"
  exit 1
fi

landscape_root="$(pwd)"

cat "${landscape_root}/landscape/generated/kubeconfig-${CLUSTERNAME}-reg.yaml"
export KUBECONFIG=${landscape_root}/landscape/generated/kubeconfig-${CLUSTERNAME}-reg.yaml



ENDS=$((SECONDS+7200))
WORKLOAD_CLUSTER_READY=false
while [ $SECONDS -lt $ENDS ]; do
    CURRENT_STATUS=`kubectl get cluster -n default -o json | jq .items[0].status.lastStage -r`
    if [ "$CURRENT_STATUS" != "ready" ]; then
      echo "Current status is $CURRENT_STATUS, waiting for workload cluster become ready..."
      sleep 60s
    else
      WORKLOAD_CLUSTER_READY=true
      break
    fi
done

if $WORKLOAD_CLUSTER_READY; then
  kubectl get cluster -n default
  kubectl get cluster -n default | grep ${CLUSTERNAME} | awk '{print $1}'
  export CLUSTER=$(kubectl get cluster -n default | grep ${CLUSTERNAME} | awk '{print $1}')
  echo "Cluster Name: $CLUSTER"
  kubectl -n default get secret $CLUSTER -o json | jq '.data' | jq '.kubecfg' -r | base64 -d > $(pwd)/kubeconfig-${CLUSTERNAME}.yaml
  echo "Kubeconfig for shoot cluster: $CLUSTER"
  cat $(pwd)/kubeconfig-${CLUSTERNAME}.yaml
  export KUBECONFIG=$(pwd)/kubeconfig-${CLUSTERNAME}.yaml
  echo ${KUBECONFIG}
  kubectl get ns
  kubectl get po -n datahub
else
  echo "Workload cluster is not ready in 2 hours"
  exit 1
fi