#!/usr/bin/env python

import sys, xml.etree.ElementTree as ET

path = sys.argv[1]
name = sys.argv[2]

with open(path) as f:
    data = f.read()

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
