def find(parent,i):
    if parent[i]==i:
        return i
    return find(parent,parent[i])

def union(parent,rank,x,y):
    xroot = find(parent,x)
    yroot = find(parent,y)

    if rank[xroot] < rank[yroot]:
        parent[xroot] = yroot
    elif rank[xroot] > rank[yroot]:
        parent[yroot] = xroot
    else:
        parent[yroot] = xroot
        rank[xroot] += 1

def boruvkaMST(graph,V):
    parent=[]
    rank=[]
    cheapest=[]
    numTrees = V
    MSTweight = 0

    for node in range(V):
        parent.append(node)
        cheapest.append(-1)
        rank.append(0)

    while numTrees > 1:
        for i in range(len(graph)):
            u,v,w = graph[i]
            set1 = find(parent,u)
            set2 = find(parent,v)

            if set1 != set2:
                if cheapest[set1] == -1 or cheapest[set1][2] > w:
                    cheapest[set1] = (u,v,w)

                if cheapest[set2] == -1 or cheapest[set2][2] > w:
                    cheapest[set2] = (u,v,w)

        for node in range(V):
            if cheapest[node] != -1:
                u,v,w = cheapest[node]
                set1 = find(parent,u)
                set2 = find(parent,v)

                if set1 != set2:
                    MSTweight += w
                    union(parent,rank,set1,set2)
                    print(f"Edge {u}-{v} with weight {w} included in MST")
                    numTrees = numTrees - 1

        cheapest = [-1] * V
    print("Weight of MST is:",MSTweight)


graph=[(0, 1, 10)
,(0, 2, 6)
,(0, 3, 5)
,(1, 3, 15)
,(2, 3, 4)]

V=4
boruvkaMST(graph,V)