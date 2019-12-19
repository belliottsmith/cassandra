#!/usr/bin/env python

import sys, xml.etree.ElementTree as ET

path = sys.argv[1]
name = sys.argv[2]

with open(path) as f:
    data = f.read()

if len(data) == 0:
    sys.exit(0)

try:
    suite = ET.fromstring(data)

    # The expected input is the class name (package.name);
    # Rio groups based off this attribute, so if the class name is present, its seen as the "package" in quality, and has a sub-section which repeats the class.
    # To better match the behavior of Jenkins, drop the class name in favor of the package name, this will have Quality group by the package
    class_name = suite.attrib['name']
    package_name = ".".join(class_name.split(".")[:-1])

    # prefix using the provided name, so quality can group properly
    suite.attrib['name'] = name + "." + package_name

    with open(path, "w") as f:
        f.write( ET.tostring(suite) )
except ET.ParseError:
    # If ant is killed while it is running tests, only the test results so far are written to the
    # junit report file and the XML file is not terminated correctly. In that case accept
    # that we cannot update the suitename and let Rio/Jenkins handle it.
    sys.stderr.write("Unable to parse file '{}'. Assuming partial write due to timeout. Contents: \n".format(name, path))
except:
    sys.stderr.write("Unable replace suitename {} in file '{}'. Contents: \n".format(name, path))
    sys.stderr.write(str(data))
    raise
