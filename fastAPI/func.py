from fastapi import HTTPException
import rdflib
from rdflib.extras.external_graph_libs import rdflib_to_networkx_multidigraph
import networkx as nx
import matplotlib.pyplot as plt
import re
from random import sample
import graphdb
import pandas as pd
import numpy as np
from SPARQLWrapper import SPARQLWrapper, JSON
from pyvis.network import Network
import random
import requests
import urllib.request
import urllib3
import urllib.parse
import json 
from rdflib import Graph
import operator

import networkx as nx


prefixes="""PREFIX esadm: <http://vocab.linkeddata.es/datosabiertos/def/sector-publico/territorio#>
PREFIX geosparql: <http://www.opengis.net/ont/geosparql#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX sf: <http://www.opengis.net/ont/sf#>
PREFIX slodbi: <http://krono.act.uji.es/schemas/slodbi#>
PREFIX vcard: <http://www.w3.org/2006/vcard/ns#>
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX ns1: <http://krono.act.uji.es/vocabulary/slodbi#>
PREFIX prov: <http://www.w3.org/ns/prov#> 
"""
template1=' SELECT ?s ?o WHERE {{ ?s {} ?o.}} LIMIT 150'
template2=' SELECT ?s ?o ?oo WHERE {{ ?s {} ?o. ?o {} ?oo. }} LIMIT 10'
template3="""              
        SELECT ?o ?s3 WHERE{{
        ?o rdfs:label {}.
        ?o {} ?s2.
        ?s2 vcard:fn ?s3.
        ?s2 rdf:type ?type.
        }}
"""
#problem when there is a line break between the SELECT ... and there WHERE {{}}
#see the regex re.compile("SELECT(.*?){").search(query).group(1)

# problem with current version: can't have two {} of the same value -> include {1} {2} etc ||| should be able to choose selectors 
t1vars=[{"default":"?p","options":{"slodbi:tiene-hoteles":"hotels","slodbi:tiene-albergues":"hostels","slodbi:tiene-empresas-de-turismo-activo":"entreprises","slodbi:tiene-campings":"campings"}}]
t2vars=[{"default":"?p","options":{"slodbi:tiene-hoteles":"hotels","slodbi:tiene-albergues":"hostels","slodbi:tiene-empresas-de-turismo-activo":"entreprises","slodbi:tiene-campings":"campings"}},{"default":"?pp","options":{"vcard:fn":"label","slodbi:habitaciones":"habitaciones"}}]
t3vars=[{"default":"?location","options":{"'Peñíscola'":"Peñíscola","'Oropesa del Mar'":"Oropesa del Mar","'Alcalà de Xivert'":"Alcalà de Xivert"}},{"default":"?predicate","options":{"slodbi:tiene-hoteles":"hotels","slodbi:tiene-albergues":"hostels","slodbi:tiene-empresas-de-turismo-activo":"entreprises","slodbi:tiene-campings":"campings"}}]


templates=[{"index":0,"queryTemplate":template1,"description":"get all [predicate]","variables":t1vars},{"index":1,"queryTemplate":template2,"description":"get all edges of length 2 with predicates [1] and [2]","variables":t2vars},{"index":2,"queryTemplate":template3,"description":"get all buildings in [location] of type [type]","variables":t3vars}]


G = nx.DiGraph()

typesDic={}
nodeTypes=[]
####################################################################################

def get_repositories():
    x = requests.get("http://localhost:7200/rest/repositories")
    return x.content


#everything is currently done on the repository tach4, but we can include a selector to choose the repository Id of our choosing
sparql = SPARQLWrapper(
        "http://LAPTOP-CS19OKIS:7200/repositories/tach4"
    )
sparql.setReturnFormat(JSON)

def answer_df(query):
    print("query sent :  "+prefixes+query)
    sparql.setQuery(prefixes+query)

    try:
        ret = sparql.queryAndConvert()     
    except Exception as e:
        print(e)

    dataframe=pd.DataFrame(ret["results"]["bindings"])
    cleandata={}

    for l in dataframe.columns:
        cleandata[l]=[]
    
    for r in ret["results"]["bindings"]:
        for l in dataframe.columns:
            cleandata[l].append(r[l]["value"])

    dataframe=pd.DataFrame(cleandata)
    reccordDic=dataframe.to_json(orient="records")
    reccordDic = json.loads(reccordDic)
    return reccordDic

#{"query":query,"reponse":reccordDic}

def query_fromTemplate(templateId,varList):
    if (varList is None):
        print("varList empty")
    else:
        query=templates[templateId]["queryTemplate"].format(*varList)
        return query
    return None

def answer_df_fromTemplate(templateId,varList):
    if (query_fromTemplate(templateId,varList) is not None):
        query=templates[templateId]["queryTemplate"].format(*varList)
        #étoile pour dire qu'on passe un argument sequence
        return answer_df(query)   
    return None
    

def getTemplates():
    return templates

def getTemplate(id):
    return templates[id]





# --------------------- POST FUNCTIONS ----------------------------



def createJDBCview(repoId,niewName,templateId,varList):
    newViewBody=viewCreationBodyGenerator(niewName,prefixes+query_fromTemplate(templateId,varList))
    print("\nbody i sent : \n")
    print(newViewBody)

    repoheader = { "X-GraphDB-Repository" : repoId }
    print("\nheader i sent: \n")
    print(repoheader)

    r = requests.post('http://localhost:7200/rest/sql-views/tables/',json=newViewBody,headers=repoheader)

    if r.status_code != 201:
        raise HTTPException(status_code=418, detail="view not created",headers={"graphDB-Status":r.status_code,"graphDB-Error":r.text})

    
    return r.status_code

# --header 'X-GraphDB-Repository: tach4' to specify a rep destination: active repository instead
# we could include a repository selector for the UI at the begining 
# in this prototype we're using tach4

def viewCreationBodyGenerator(viewName,query):
      
    # 1==>findall "?varI" in the "SELECT ?v1 ?v2 ..." section of the query string  
    print(query)
    varSectionString=re.compile("SELECT(.*?){").search(query).group(1)
    #gets us the string with all selected variables 
    selectVariables=re.findall('\?([^\s]*)',varSectionString)
    #gets us a list of the variable names

    # 2==> add \n #!filter \n for api format expectation 
    
    endOfQuery=re.compile("\s*(\}.*?)$").search(query).group()
    #find the end of query " }..." string 
    query=query.replace(endOfQuery,"\n #!filter \n"+endOfQuery)
    #add "#!filter" before
    
    columns=[]
    for v in selectVariables:
        columns.append({
          "column_name": v,
          "column_type": "string",
          "nullable": True
        })
    
    tosend={
      "name": viewName,
      "query": query,
      "columns": columns,
    }

    return tosend



    ########################### GRAPH METRICS #############################


def cleanDataFrame(dataset):  
    if 'p' in dataset.columns:
        listeu=dataset["p"]
        dataset=dataset[-dataset["o"].isin(listeu)]
        dataset=dataset[-dataset["s"].isin(listeu)]
        # remove nodes that are also predicates 
        dataset=dataset[-dataset["o"].str.match(".*www.w3.org.*")]
        dataset=dataset[-dataset["s"].str.match(".*www.w3.org.*")]
        # remove nodes of www.w3.org because they are noisy w3 properties 
    
    #==> remove the url start "http//.../(.)"
    pat = r'.*\/(.)'
    dataset["s"]= dataset["s"].str.replace(pat,r'\1',regex=True)
    dataset["o"]= dataset["o"].str.replace(pat,r'\1',regex=True)

    dataset["o"]=dataset["o"].str.replace(".*#(.)",r'\1',regex=True)
    dataset["s"]=dataset["s"].str.replace(".*#(.)",r'\1',regex=True)

    return dataset


def get_graph_df():
    
    sparql.setQuery("SELECT ?o ?p ?s WHERE { ?o ?p ?s.}")

    try:
        ret = sparql.queryAndConvert()     
    except Exception as e:
        print(e)

    dataframe=pd.DataFrame(ret["results"]["bindings"])
    newdata={}

    for l in dataframe.columns:
        newdata[l]=[]
    
    for r in ret["results"]["bindings"]:
        for l in dataframe.columns:
            newdata[l].append(r[l]["value"])

    dataframe=pd.DataFrame(newdata)
    return dataframe

def cleanForNXGraph(dataset):  
    cleanedDf=dataset[-(dataset["p"]=="http://www.w3.org/1999/02/22-rdf-syntax-ns#type")]

    return cleanedDf

def initializeGraph():
    df=get_graph_df()
    cleandf=cleanDataFrame(df)

    #df of predicate type
    nodeTypesDf=cleandf[cleandf["p"]=="http://www.w3.org/1999/02/22-rdf-syntax-ns#type"]

    #get dictionary with the nodes their type
    global typesDic
    typesDic=nodeTypesDf[["o","s"]].set_index('o')['s'].to_dict()
    
    #get list of node types
    global nodeTypes
    nodeTypes=nodeTypesDf["s"].unique()

    #remove noisy nodes for metrics computation then type nodes,
    cleanedDF=cleanForNXGraph(cleandf) 

    global G
    print("before: ",G.size())
    G=nx.from_pandas_edgelist(cleanedDF, source='s',target='o',edge_attr='p',create_using=nx.DiGraph())
    print("after: ",G.size())


    metrics_cache.update({}.fromkeys(metrics_cache,None))
    #for m in metrics_cache:


    G.remove_nodes_from(["false","nan","Entiers","true"])

    return G.size()



metrics_cache={
    "size":None,
    "density":None,
    "transitivity":None,
}

def graphSize():
    if(G.size()==0):
        initializeGraph()
        print("graph not initialized")
    if(metrics_cache["size"] is None):
        sizeRes=G.size()
        metrics_cache["size"]=sizeRes
        return sizeRes
    return metrics_cache["size"]

def graphDensity():
    if(G.size()==0):
        initializeGraph()
        print("graph not initialized")
    if(metrics_cache["density"] is None):
        densityRes=nx.density(G)
        metrics_cache["density"]=densityRes
        return densityRes
    return metrics_cache["density"]

def graphTransitivity():
    if(G.size()==0):
        initializeGraph()
        print("graph not initialized")
    if(metrics_cache["transitivity"] is None):
        transitivityRes=nx.transitivity(G)
        metrics_cache["transitivity"]=transitivityRes
        return transitivityRes
    return metrics_cache["transitivity"]


def node_metric_type_tuple(metric_func,nb_res):
    if(G.size()==0):
        initializeGraph()
    to_sort=[]
    for tuple in metric_func(G).items():
        to_sort.append(
            (*tuple,
             (typesDic.get(tuple[0],None))
            )
        )

    sorted_tuples = sorted(to_sort, key=operator.itemgetter(1),reverse=True)
    return sorted_tuples[:nb_res]

def get_sorted_degree(nb_res):
    if(G.size()==0):
        initializeGraph()
    sorted_degree = sorted(dict(G.degree(G.nodes())).items(), key=operator.itemgetter(1),reverse=True)
    res=[]
    for m in sorted_degree:
        res.append((*m,typesDic.get(m[0],None)))

    return res[:nb_res]

def get_sorted_degree_centrality(nb_res):
    return node_metric_type_tuple(nx.degree_centrality,nb_res)

def get_sorted_in_degree_centrality(nb_res):
    return node_metric_type_tuple(nx.in_degree_centrality,nb_res)

def get_sorted_out_degree_centrality(nb_res):
    return node_metric_type_tuple(nx.out_degree_centrality,nb_res)

def get_sorted_eigenvector_centrality(nb_res):
    return node_metric_type_tuple(nx.eigenvector_centrality,nb_res)


def get_type_from_name(name):
    return typesDic[name]

def get_types_dic():
    return typesDic