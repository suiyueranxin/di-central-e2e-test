import control_center
import os

cc_token = control_center.login_cc()
shoot_name = os.environ["SHOOT_NAME"]
kubeconfig = control_center.get_DHaaS_kubecfg_by_shootname(cc_token, shoot_name)
with open('./kubeconfig.yaml', 'w') as f:
    f.write(kubeconfig)