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


def _get_test_set_by_keywords(test_info_file, module_file, kws_str):       
    test_set = []
    with open(test_info_file) as f:
        test_name_2_test_info = json.load(f)
    module_name = module_file.split(".")[0]
    test_info = test_name_2_test_info.get(module_name)
    for test_method_name in test_info:
        if not isinstance(test_info.get(test_method_name), str):
            the_case_kws_str = test_info.get(test_method_name).get("priority")
            if _is_keywords_match(the_case_kws_str, kws_str):
                test_set.append(test_method_name)
    return test_set


if __name__ == "__main__":
    usage = "python %prog --start-dir <start_folder> --output <target_xml_reports_fold> --test-info-json <test_name_2_priority_config> --key-test-name <key_test_name> --priority <test_priority> "
    parser = optparse.OptionParser(usage)
    parser.add_option("--start-dir", "-s", dest="start_folder", type="string", default="")
    parser.add_option("--output", "-o", dest="output", type="string", default="testresult")
    parser.add_option("--test-info-json", "-j", dest="test_info_json", type="string", default="test_info.json")
    parser.add_option("--file", "-f", dest="file", type="string", default="test_*.py")
    parser.add_option("--key-test-name", "-k", dest="key_test_name", type="string", default="")
    parser.add_option("--priority", "-p", dest="priority", type="string", default="P1")
    
    options, args = parser.parse_args()
    start_folder = options.start_folder
    output = options.output
    test_name_2_priority_config = options.test_info_json
    file = options.file
    key_test_name = options.key_test_name
    priority = options.priority
    
    test_set = _get_test_set_by_keywords(test_name_2_priority_config, file, priority)
    
    loader = unittest.defaultTestLoader
    discover = loader.discover(start_folder, pattern=file)
    result_suite = unittest.TestSuite()
    for suite in discover._tests[0]:
        for test in suite._tests:
            if key_test_name in test._testMethodName and test._testMethodName in test_set:
                result_suite.addTest(test)
    runner = xmlrunner.XMLTestRunner(output=output)
    ret = not runner.run(result_suite).wasSuccessful()
    sys.exit(ret)
    