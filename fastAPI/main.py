

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware


from typing import List, Union
from func import answer_df, answer_df_fromTemplate, createJDBCview, get_repositories, get_sorted_degree_centrality, get_sorted_eigenvector_centrality, get_sorted_in_degree_centrality, get_sorted_out_degree_centrality, get_type_from_name, getTemplate, getTemplates, graphDensity, graphSize, graphTransitivity, initializeGraph, get_sorted_degree
#from postfunc import createJDBCview

description="""
This API serves as the backend for the Knowledge Graph Analysis User Interface\n
Project under the supervision of Ismael Sanz

## Items

You can...
"""

tags_metadata = [
    {
        "name": "repositories",
        "description": "Operations related to the graphDB repositories are here."
    },
    {
        "name": "query",
        "description": "Operations related to query generation, execution etc. ",
    },
    {
        "name": "graph",
        "description": "Operations related to the graph generation and metrics computation are here."
    }
]

contact={
    "name":"Selim Gmati",
    "email":"selim.gmati@insa-rennes.fr"
}



app = FastAPI(
    title="GraphUI API",
    description=description,
    contact=contact,
    openapi_tags=tags_metadata
)


origins = [
    "http://localhost",
    "http://localhost:8080",
    "http://localhost:4200"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], #POURQUOI CA MARCHE PAS AVEC [origins]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/repositories",tags=["repositories"])
async def repositories():
    return  get_repositories()


@app.get('/query',tags=["query"])
async def queryExample():
    return  answer_df("SELECT ?s ?p ?o WHERE {?s ?p ?o.} LIMIT 10")

@app.get('/query/{templateId}/',tags=["query"])
async def queryTemplate(templateId:int,varList: List[str]= Query(default=None)):
    return  answer_df_fromTemplate(templateId,varList)

@app.get('/templates',tags=["query"])
async def templates():
    return getTemplates()

@app.get('/templates/{id}/',tags=["query"])
async def testeu(id:int):
    return getTemplate(id)

@app.post('/{repoId}/sqlviews/{templateId}/{viewName}',tags=["query"])
async def queryTemplate(repoId:str,templateId:int,viewName:str,varList: List[str]):
    print("wtf")
    print(varList)
    return createJDBCview(repoId,viewName,templateId,varList)
    ##maybe change this request body ? param ? query(blabla) ??? look up post requests 



################################ METRICS #######################################

#########ini####################

@app.post("/graph",tags=["graph"])
async def graphData():
    return initializeGraph()

########general metrics#########
@app.get("/graph/metrics/size",tags=["graph"])
async def graphDataSize():
    return graphSize()

@app.get("/graph/metrics/density",tags=["graph"])
async def graphDataDensity():
    return graphDensity()

@app.get("/graph/metrics/transitivity",tags=["graph"])
async def graphDataTransitivity():
    return graphTransitivity()

########sorted nodes metrics####

@app.get("/graph/metrics/sorted/degree/{nb_res}/",tags=["graph"])
async def sorted_degree(nb_res:int=10):
    return get_sorted_degree(nb_res)


@app.get("/graph/metrics/sorted/centrality/{nb_res}/",tags=["graph"])
async def sorted_degree_centrality(nb_res:int=10):
    return get_sorted_degree_centrality(nb_res)

@app.get("/graph/metrics/sorted/centrality/in/{nb_res}/",tags=["graph"])
async def sorted_in_degree_centrality(nb_res:int=10):
    return get_sorted_in_degree_centrality(nb_res)

@app.get("/graph/metrics/sorted/centrality/out/{nb_res}/",tags=["graph"])
async def sorted_out_degree_centrality(nb_res:int=10):
    return get_sorted_out_degree_centrality(nb_res)

@app.get("/graph/metrics/sorted/centrality/eigenvector/{nb_res}/",tags=["graph"])
async def sorted_eigenvector_centrality(nb_res:int=10):
    return get_sorted_eigenvector_centrality(nb_res)


#############TEST######################

@app.get("/types/{name}/",tags=["graph"])
async def get_type(name:str):
    return get_type_from_name(name)

@app.get("/types",tags=["graph"])
async def get_types_dic():
    return get_types_dic()