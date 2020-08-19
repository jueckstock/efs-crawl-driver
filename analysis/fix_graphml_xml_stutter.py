#!/usr/bin/env python3
import sys
import xml.etree.ElementTree as ET


def main(argv):
    for filename in argv[1:]:
        tree = ET.parse(filename)
        root = tree.getroot()
        
        edge_type_attr = root.find(".//{http://graphml.graphdrawing.org/xmlns}key[@for='edge'][@attr.name='edge type']")
        assert edge_type_attr is not None, "no 'edge type' edge attribute defined?"
        edge_type_key_id = edge_type_attr.attrib['id']

        value_attr = root.find(".//{http://graphml.graphdrawing.org/xmlns}key[@for='edge'][@attr.name='value']")
        assert value_attr is not None, "no 'value' edge attribute defined?"
        value_key_id = value_attr.attrib['id']

        edge_xpath = f".//{{http://graphml.graphdrawing.org/xmlns}}data[@key='{edge_type_key_id}'][.='request complete'].."
        for edge in root.findall(edge_xpath):
            whack = edge.findall(f"{{http://graphml.graphdrawing.org/xmlns}}data[@key='{value_key_id}']")
            for w in whack[1:]:
                edge.remove(w)
        
        tree.write(filename)


        


if __name__ == "__main__":
    main(sys.argv)
