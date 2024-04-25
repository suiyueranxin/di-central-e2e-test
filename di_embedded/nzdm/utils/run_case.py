import unittest
import sys
import xmlrunner
import optparse
import os
import json 
#  Add the project's root directory into sys.path
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../')))

def _is_keywords_match(src_kws_str, target_kws_str):
    if src_kws_str is None or target_kws_str is None:
        return True
    src_set = set(src_kws_str.split(";"))
    target_set = set(target_kws_str.split(";"))
    return src_set.issubset(target_set)


def _filter_test_set_by_stage(test_info_file, stage, limited_test_set):       
    test_set = []
    with open(test_info_file) as f:
        test_name_2_test_info = json.load(f)
    for test_folder in test_name_2_test_info:
        for test_method_name in test_name_2_test_info.get(test_folder):
            cur_stage = test_name_2_test_info.get(test_folder).get(test_method_name).get("stage")
            if (len(limited_test_set)==0 or test_method_name in limited_test_set) and (cur_stage == "all" or stage in cur_stage):
                test_set.append(test_method_name)
    return test_set


if __name__ == "__main__":
    usage = "python %prog --start-dir <start_folder> --output <target_xml_reports_fold> --test-info-json <test_info_config> --key-test-name <key_test_name> --stage <upgrade_stage> --limited-test-set <test_set>"
    parser = optparse.OptionParser(usage)
    parser.add_option("--start-dir", "-s", dest="start_folder", type="string", default=".")
    parser.add_option("--output", "-o", dest="output", type="string", default="testresult")
    parser.add_option("--test-info-json", "-j", dest="test_info_json", type="string", default="test_info.json")
    parser.add_option("--key-test-name", "-k", dest="key_test_name", type="string", default="test")
    parser.add_option("--stage", "-p", dest="stage", type="string", default="pre-upgrade")
    parser.add_option("--limited-test-set ", "-l", dest="limited_test_set", type="string", default="")
    
    options, args = parser.parse_args()
    start_folder = options.start_folder
    output = options.output
    test_info_config = options.test_info_json
    key_test_name = options.key_test_name
    stage = options.stage
    limited_test_set = options.limited_test_set.split(",")

    # os.environ.setdefault("stage", stage)
    test_set = _filter_test_set_by_stage(test_info_config, stage, limited_test_set)
 
    loader = unittest.defaultTestLoader
    discover = loader.discover(start_folder)
    result_suite = unittest.TestSuite()
    for parent_suite in discover._tests:
        for suite in parent_suite._tests:
            for test in suite._tests:
                if key_test_name in test._testMethodName and test._testMethodName in test_set:
                    result_suite.addTest(test)
    runner = xmlrunner.XMLTestRunner(output=output)
    ret = not runner.run(result_suite).wasSuccessful()
    sys.exit(ret)
    