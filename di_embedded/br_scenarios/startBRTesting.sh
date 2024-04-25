#!/bin/bash

# Prepare the parameters 
sourceCluster=$1
clusterName=$2
# rm -rf region_kc.txt
# if [[ "${sourceCluster}" == *"-aws-"* ]]; then
#     cp kubeconfig/aws_region_kc.txt region_kc.txt
# elif [[ "${sourceCluster}" == *"-az-"* ]]; then
#     cp kubeconfig/az_region_kc.txt region_kc.txt
# else
#     exit 1
# fi
# KUBECONFIG=$(pwd)/region_kc.txt
echo "clusterName is ${clusterName}"
echo "sourceCluster is ${sourceCluster}"
regionKubeconfigPath=$(pwd)/landscape/generated/kubeconfig-${clusterName}-reg.yaml
workloadKubeconfigPath=$(pwd)/kubeconfig-${clusterName}.yaml
cd ./di_embedded/br_scenarios
export KUBECONFIG=${regionKubeconfigPath}
echo "kubeconfig path is ${KUBECONFIG}"
shootName=$(kubectl get clusters -n default -o json | jq -c -r --arg RCLUSTERNAME $sourceCluster '.items[] | select( .metadata.name | contains($RCLUSTERNAME))' | jq .spec.shootName)
shootName=$(echo "$shootName" | tr -d '"')
#url="https://vsystem.ingress.${shootName}.di-demo.shoot.canary.k8s-hana.ondemand.com"
endpoint=$(kubectl -n default get cluster ${sourceCluster} -o json | jq '.status' | jq '.vsystemIngressHostName')
endpoint=$(echo "$endpoint" | tr -d '"')
url=https://${endpoint}
echo "endpoint is ${endpoint}"
echo "url is ${url}"
echo "Cluster full name is ${sourceCluster}"

# create kubeconfig for workload cluster
# echo "##start creating kubeconfig for workload clusters"
# cp temp/temp_kc.txt workloadCluster_${shootName}_kc.txt
# sed -i "s/<shoot_name>/${shootName}/g" workloadCluster_${shootName}_kc.txt

# create tenant
# echo "##start creating tenant for workload clusters"
# echo $(pwd)
# if [[ "${sourceCluster}" == *"-aws-"* ]]; then
#   export BROKER_URL='https://di-broker.ingress.reg.di-dmo-aws.aws.di-demo.hanacloud.ondemand.com'
#   export BROKER_USERNAME='dis-broker'
#   export BROKER_PASSWORD='uJK]BPl*=Q7Ug2#>gx}Uj+lC_wEbia'
# else
#   export BROKER_URL='https://di-broker.ingress.reg.di-dmo-az.azure.di-demo.hanacloud.ondemand.com'
#   export BROKER_USERNAME='dis-broker'
#   export BROKER_PASSWORD='uJK]BPl*=Q7Ug2#>gx}Uj+lC_wEbia'
# fi
# export BROKER_URL=$(kubectl -n default get ingress service-broker -o json | jq .spec.rules[0].host)
# export BROKER_URL=https://$(echo "$BROKER_URL" | tr -d '"')
# export BROKER_USERNAME=$(kubectl -n default get secret dis-broker-creds -o json | jq .data.username | base64 -di)
# export BROKER_PASSWORD=$(kubectl -n default get secret dis-broker-creds -o json | jq .data.password | base64 -di)
# python3 broker-helper.py provision -r ${sourceCluster}
# python3 broker-helper.py provision -r ${sourceCluster}
# echo "Waiting for tenant created"
# sleep 12m

# get username and password
# IFS=' ' read -r -a tenantInfoArray <<< $tenantList
# tenantName0=$(echo ${tenantInfoArray[0]} | tr -d '"')
# tenantName1=$(echo ${tenantInfoArray[1]} | tr -d '"')
# tenantList=$(kubectl get tenant -n default -o json | jq -c -r --arg SOURCECLUSTER $sourceCluster '.items[] | select(.spec.clusterID | contains($SOURCECLUSTER))' | jq .metadata.name)
# SAVEIFS=$IFS
# tenantList=(${tenantList})
# IFS=$'\r'
# IFS=$SAVEIFS
# tenantName0=$(echo ${tenantList[0]} | tr -d '"')
# tenantName1=$(echo ${tenantList[1]} | tr -d '"')
# tenantName0=($(kubectl get tenant -n default -o json | jq -c -r --arg SOURCECLUSTER $sourceCluster '.items[] | select(.spec.clusterID | contains($SOURCECLUSTER))' | jq .metadata.name))
# tenantName1=$(kubectl get tenant -n default -o json | jq -c -r --arg SOURCECLUSTER $sourceCluster '.items[] | select(.spec.clusterID | contains($SOURCECLUSTER))' | jq .metadata.name)
# tenantName0=$(echo ${tenantName0} | tr -d '"')
# tenantName1=$(echo ${tenantName1} | tr -d ' ' | tr -d '"')

# username=$(kubectl get secret -n default "${sourceCluster}-${tenantName0}" -o jsonpath='{.data.username}' | base64 -d)
# password=$(kubectl get secret -n default "${sourceCluster}-${tenantName0}" -o jsonpath='{.data.password}' | base64 -d)
# echo "Username0 is ${username}"
# echo "Password0 is ${password}"

# username1=$(kubectl get secret -n default "${sourceCluster}-${tenantName1}" -o jsonpath='{.data.username}' | base64 -d)
# password1=$(kubectl get secret -n default "${sourceCluster}-${tenantName1}" -o jsonpath='{.data.password}' | base64 -d)
# echo "Username1 is ${username1}"
# echo "Password1 is ${password1}"
# echo "tenant status is"
# kubectl get tenant -n default | grep ${sourceCluster}

# run pre backup test from python
echo "pre_backup_testing.py ${sourceCluster} ${shootName} ${url} ${tenantName0} ${username} ${password}"
python $(pwd)/tests/pre_backup_testing.py ${sourceCluster} ${shootName} ${url} ${tenantName0} ${username} ${password}

echo "pre_backup_testing.py ${sourceCluster} ${shootName} ${url} ${tenantName1} ${username1} ${password1}"
python $(pwd)/tests/pre_backup_testing.py ${sourceCluster} ${shootName} ${url} ${tenantName1} ${username1} ${password1}



# create backup file
echo "##start doing backup operator backup"
export KUBECONFIG=${regionKubeconfigPath}

cp $(pwd)/temp/backup_temp.yaml $(pwd)/backup_${shootName}.yaml
backupName="backup-${sourceCluster}"
sed -i "s/<backupName>/$backupName/g" backup_${shootName}.yaml
sed -i "s/<clusterName>/$sourceCluster/g" backup_${shootName}.yaml

# run backup operator backup
kubectl apply -f backup_${shootName}.yaml -n default
echo "Waiting for operator backup generator"
sleep 20s

# check backup job ready
backupJobStatus=$(kubectl get dhb -n default -o json | jq -c -r --arg BACKUPNAME $backupName '.items[] | select(.metadata.name | contains($BACKUPNAME))' | jq .status.state)
echo "operator backup job status is ${backupJobStatus}"
if [[ "${backupJobStatus}" == "failed" ]]; then
    echo "take backup operator backup failed!!!"
    exit 1
fi


# run datahub operator backup 
echo "##start doing datahub operator backup"
export KUBECONFIG=${workloadKubeconfigPath}
kubectl exec -it datahub-operator-0 -n datahub-system /bin/bash -- dhinstaller backup
sleep 3m
backupItemsCount=$(kubectl get backup -o json -n datahub --sort-by=.metadata.creationTimestamp | jq .items | jq length)
backupItemsCount="$(($backupItemsCount - 1))"
timestamp=$(kubectl get backup -n datahub -o json --sort-by=.metadata.creationTimestamp | jq -r --arg INDEX $backupItemsCount '.items[$INDEX|tonumber].metadata.annotations["com.sap.datahub.installers.backup.timestamp"]')
echo "The latest timestamp is ${timestamp}"

if [[ "${timestamp}" == "null" ]]; then
    sleep 12h
fi

# stop workload cluster
echo "##stop workload cluster"
export KUBECONFIG=${workloadKubeconfigPath}
kubectl exec -n datahub-system datahub-operator-0 -- dhinstaller runlevel -r Stopped -n datahub

# create restore yaml
echo "##start restore cluster"
export KUBECONFIG=${regionKubeconfigPath}
resotredClusterName=res-${sourceCluster}
cp ./temp/restore_temp.yaml ./restore_${shootName}.yaml
sed -i "s/<backupName>/$backupName/g" ./restore_${shootName}.yaml
sed -i "s/<timestamp>/$timestamp/g" ./restore_${shootName}.yaml
sed -i "s/<resotredClusterName>/$resotredClusterName/g" ./restore_${shootName}.yaml

# doing restoration
export KUBECONFIG=${regionKubeconfigPath}
cat restore_${shootName}.yaml
kubectl apply -f restore_${shootName}.yaml -n default
echo "Waiting for restored cluster ready"
restoreStatus="pending"
waitingTime=0
while [ "${restoreStatus}"  != "ready" ]
do
 sleep 1m
 restoreStatus=$(kubectl get clusters -n default -o json | jq -c -r --arg RESTOREDCLUSTER $resotredClusterName '.items[] | select(.metadata.name | contains($RESTOREDCLUSTER))' | jq .status.lastStage)
 restoreStatus=$(echo "$restoreStatus" | tr -d '"')
 echo "Current status is $restoreStatus"
 waitingTime="$(($waitingTime + 1))"
 if [[ "$waitingTime" == "120" ]]; then
    echo "Cluster is not ready in 120 mins."
    exit 1
 fi
done

# get new cluster shootname
export KUBECONFIG=${regionKubeconfigPath}

restoreEndpoint=$(kubectl -n default get cluster ${resotredClusterName} -o json | jq '.status' | jq '.vsystemIngressHostName')
restoreEndpoint=$(echo "$restoreEndpoint" | tr -d '"')
resotredURL=https://${restoreEndpoint}
echo "restore endpoint is ${restoreEndpoint}"
echo "restore url is ${resotredURL}"

# run post restore test from python
# echo "post_restore_testing.py ${sourceCluster} ${resotredURL} ${tenantName0} ${username} ${password}"
# python $(pwd)/post_restore_testing.py ${sourceCluster} ${resotredURL} ${tenantName0} ${username} ${password}
# echo "post_restore_testing.py ${sourceCluster} ${resotredURL} ${tenantName1} ${username1} ${password1}"
# python $(pwd)/post_restore_testing.py ${sourceCluster} ${resotredURL} ${tenantName1} ${username1} ${password1}

pip uninstall -y xmlrunner 

pip install unittest-xml-reporting

export CLUSTERNAME=${sourceCluster}
export URL=${resotredURL}
export TENANT=${tenantName0}
export USERNAME=${username}
export PASSWORD=${password}
python -m xmlrunner discover -s ./ -o test-reports -p post_restore_testing.py

export CLUSTERNAME=${sourceCluster}
export URL=${resotredURL}
export TENANT=${tenantName1}
export USERNAME=${username1}
export PASSWORD=${password1}
python -m xmlrunner discover -s ./ -o test-reports -p post_restore_testing.py

# force delete the workload cluster
echo "##delete workload cluster"
export KUBECONFIG=${workloadKubeconfigPath}
chmod +x dhinstaller
./dhinstaller uninstall --force
# sleep 12h to debug the restoration
# echo "###sleep 12h to debug the issue.####"
# sleep 12h

# clean env file
rm -rf $(pwd)/workloadCluster_${shootName}_kc.txt
rm -rf ./backup_${shootName}.yaml
rm -rf ./restore_${shootName}.yaml

echo $(pwd)
ls -lah
cd ..
echo $(pwd)
ls -lah