import json
import optparse
import os
import xml.etree.ElementTree as ET


def _generate_report_with_xray_info(src_xml_reports_fold, target_xml_reports_fold, test_name_2_jira_config):
    target_fold = os.path.dirname(target_xml_reports_fold)
    if (not os.path.exists(target_fold)):
        os.mkdir(target_fold)
        
    with open(test_name_2_jira_config) as f:
        test_name_2_jira_info = json.load(f)
    src_list = os.listdir(src_xml_reports_fold)
    
    for file_name in src_list:
        src_xml = os.path.join(os.path.dirname(src_xml_reports_fold), file_name)
        if os.path.isfile(src_xml) and src_xml[-4:] == ".xml":
            src_tree = ET.parse(src_xml)
            src_root = src_tree.getroot()
            for test_case in src_root.iter("testcase"):
                test_case_class_name = test_case.attrib.get("classname").split(".")[-2]
                test_case_name = test_case.attrib.get("name")
                if test_name_2_jira_info.get(test_case_class_name) is not None and \
                    test_name_2_jira_info.get(test_case_class_name).get(test_case_name) is not None:
                    test_key = test_name_2_jira_info.get(test_case_class_name).get(test_case_name).get("xray_link")
                    property_ele = ET.fromstring('<properties><property name="test_key" value="{}"/></properties>'.format(test_key))
                    test_case.append(property_ele)
                else:
                    src_root.remove(test_case)
            target_xml = os.path.join(target_fold, file_name)
            open(target_xml, "w+", encoding="UTF-8")
            src_tree.write(target_xml, xml_declaration=True)


if __name__ == "__main__":
    usage = "python %prog --src <src_xml_reports_fold> --target <target_xml_reports_fold> --jira-json <test_name_2_jira_config>"
    parser = optparse.OptionParser(usage)
    parser.add_option("--src", dest="src", type="string", default="testresults")
    parser.add_option("--target", dest="target", type="string", default="xrayresults")
    parser.add_option("--jira-json", dest="jira_json", type="string", default="test_info.json")
    
    options, args = parser.parse_args()
    src_xml_reports_fold = options.src
    target_xml_reports_fold = options.target
    test_name_2_jira_config = options.jira_json
    _generate_report_with_xray_info(src_xml_reports_fold, target_xml_reports_fold, test_name_2_jira_config)