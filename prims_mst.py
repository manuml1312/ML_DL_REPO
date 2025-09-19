import sys
import matplotlib.pyplot as plt
import networkx as nx

#Finds the vertex with the minimum key value not yet included in the MST
#Returns the index of the vertex with the minimum key

def min_key(key,mst_set):
    min_val = sys.maxsize
    min_index = None

    for v in range(len(key)):
        if key[v]<min_val and not mst_set[v]:
            min_val = key[v]
            min_index=v
    return min_index

def prim_mst(graph):
    V=len(graph)
    parent=[None]*V
    key=[sys.maxsize]*V
    key[0]=0
    mst_set=[False]*V

    #Iterates V-1 times:
    #Finds the vertex with the minimum key value using min_key() function
    for _ in range(V-1):
        u=min_key(key,mst_set)
        mst_set[u]=True

        for v in range(V):
            #Update the key and parent arrays while constructing the Minimum Spanning Tree(MST)
            if graph[u][v]>0 and not mst_set[v] and key[v]>graph[u][v]:
                #3 Key update conditions
                #a.Valid Edge -- graph[u][v]>0 checks if there's a valid edge between u and v
                #b.Duplicate detection -- Vertex v not already in mst_set
                #c.Current known minimum weight for v (stored in key[v])>weight(u,v)

                key[v]=graph[u][v]
                parent[v]=u
        print("***Partial MSTs***\n",u,v)
        display_mst(parent,graph)

    #Call display_graph to visualize the original graph and display_mst to visualize the mst
    print("*********Final MST's for the original Graph\n")
    display_graph(graph,"Original Graph")
    display_mst(parent,graph)

def display_graph(graph,title):
    G=nx.Graph()
    for i in range(len(graph)):
        for j in range(len(graph)):
            if graph[i][j]!=0:
                G.add_edge(i,j,weight=graph[i][j])

        pos=nx.spring_layout(G)
        labels=nx.get_edge_attributes(G,'weight')
        nx.draw(G,pos,with_labels=True,node_size=500,node_color='skyblue',font_weight='bold')
        nx.draw_networkx_edge_labels(G,pos,edge_labels=labels)
        plt.title(title)
        plt.show()

def display_mst(parent,graph):
    G=nx.Graph()
    for i in range(1,len(graph)):
        if parent[i] is not None: #Check if parent[i] is not None
            G.add_edge(parent[i],i,weight=graph[i][parent[i]])

    pos=nx.spring_layout(G)
    labels=nx.get_edge_attributes(G,'weight')
    nx.draw(G,pos,with_labels=True,node_size=500,node_color='lightgreen',font_weight='bold')
    nx.draw_networkx_edge_labels(G,pos,edge_labels=labels)
    plt.title("Minimum Spanning Tree")
    plt.show()
    