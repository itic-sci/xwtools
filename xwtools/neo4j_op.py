#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Copyright (C)2018 SenseDeal AI, Inc. All Rights Reserved
Author: xuwei
Email: weix@sensedeal.ai
Description:
'''
# py2neo == 2021
from .config_log import config
from py2neo import Graph, Node, Relationship, NodeMatcher


class Neo4jOp():
    def __init__(self, host='', port='', user='', password='', label=''):
        if label:
            host = config(label, 'host')
            port = config(label, 'port')
            user = config(label, 'user')
            password = config(label, 'pass')
        self._graph = Graph('bolt://' + host + ':' + port, auth=(user, password))
        self._node_map = dict()
        self._edge_map = dict()
        self._node_relation_map = dict()
        self._node_edges = dict()

    def get_relationship_types(self):
        r = self._graph.schema.relationship_types
        return r

    def get_node_labels(self):
        r = self._graph.schema.node_labels
        return r

    def merge_node(self, label, unique_key, param_dict):
        tx = self._graph.begin()
        tx.merge(Node(label, **param_dict), label, unique_key)
        tx.commit()

    def create(self, node):
        self._graph.create(node)

    def push_ogm(self, node):
        self._graph.push(node)

    def delete_by_label(self, label):
        self._graph.run("MATCH (n:%s) delete n" % label)

    def truncate_neo4j(self):
        self._graph.delete_all()

    def run_cql(self, cql):
        return self._graph.run(cql)

    # 有返回Node，无返回None，返回None的话调用Node创建节点
    def nodes_match(self, labels, **kwargs):
        _node_obj = self._graph.nodes.match(labels, **kwargs).first()
        return _node_obj

    def nodes_query(self, labels, **kwargs):
        _nodes_obj = self._graph.nodes.match(labels, **kwargs)
        return _nodes_obj

    def relations_query(self, node, r_type):
        data = self._graph.relationships.match(node, r_type)
        return data

    def query_node_by_label(self, label):
        _list_obj = list(self._graph.nodes.match(label))
        _list_dict = [dict(_obj) for _obj in _list_obj]
        return _list_dict


if __name__ == '__main__':
    op_neo4j = Neo4jOp(label='neo4j')
    r = op_neo4j.get_node_labels()
    print(r)
    r = op_neo4j.get_relationship_types()
    print(r)
    r = op_neo4j.nodes_match('Company', code='10085631')
    print(dict(r))
