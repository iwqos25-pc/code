import os

current_path = os.getcwd()
parent_path = os.path.dirname(current_path)
cluster_number = 8
datanode_number_per_cluster = 20
datanode_port_start = 17600
vir_clu_per_pys_node = 2
iftest = True

proxy_ip_list = [
    ["10.0.0.2",50405],
    ["10.0.0.3",50405],
    ["10.0.0.5",50405],
    ["10.0.0.6",50405],
    ["10.0.0.7",50405],
    ["10.0.0.8",50405],
    ["10.0.0.9",50405],
    ["10.0.0.10",50405],
    ["10.0.0.11",50405],
]
coordinator_ip = "10.0.0.4"
networkcore = [
    "10.0.0.17:17500",
    "10.0.0.18:17550"
]
networkcore_address = "10.0.0.18:17550"

if iftest:
    proxy_ip_list = [
        ["0.0.0.0",50005],
        ["0.0.0.0",50035],
        ["0.0.0.0",50065],
        ["0.0.0.0",50095],
        ["0.0.0.0",50125],
        ["0.0.0.0",50155],
        ["0.0.0.0",50185],
        ["0.0.0.0",50215],
        ["0.0.0.0",50245],
        ["0.0.0.0",50275],
        ["0.0.0.0",50305],
        ["0.0.0.0",50335],
        ["0.0.0.0",50365],
        ["0.0.0.0",50395],
        ["0.0.0.0",50425],
        ["0.0.0.0",50455],
        ["0.0.0.0",50485],
        ["0.0.0.0",50515]
    ]
    coordinator_ip = "0.0.0.0"
    networkcore = [
        "0.0.0.0:17400",
        "0.0.0.0:17500"
    ]

networkcore_address = ""
for i in range(len(networkcore)):
    if i != len(networkcore) - 1:
        networkcore_address += networkcore[i]
        networkcore_address += ","
    else:
        networkcore_address += networkcore[i]

# proxy_num == rack_number_per_cluster * cluster_number
proxy_num = len(proxy_ip_list)

#cluster_information = {cluster_id:{rack_id:{'proxy':0.0.0.0:50005,'datanode':[[ip,port],...]},...},}
cluster_information = {}
def generate_cluster_info_dict():
    index = 0
    for i in range(cluster_number / vir_clu_per_pys_node):
        for j in range(vir_clu_per_pys_node):
            new_cluster = {}
            if iftest:
                new_cluster["proxy"] = proxy_ip_list[i][0] + ":" + str(proxy_ip_list[i * vir_clu_per_pys_node + j][1])
            else:
                new_cluster["proxy"] = proxy_ip_list[i][0] + ":" + str(proxy_ip_list[i][1] + j * 50)
            datanode_list = []
            for k in range(datanode_number_per_cluster):
                port = datanode_port_start + i * 100 + j * 50 + k
                datanode_list.append([proxy_ip_list[i][0], port])
            new_cluster["datanode"] = datanode_list
            cluster_information[i * vir_clu_per_pys_node + j] = new_cluster
        
            
def generate_run_proxy_datanode_file():
    file_name = parent_path + '/run_proxy_datanode.sh'
    with open(file_name, 'w') as f:
        f.write("pkill -9 run_datanode\n")
        f.write("pkill -9 run_proxy\n")
        f.write("\n")
        for cluster_id in cluster_information.keys():
            print("cluster_id",cluster_id)
            for each_datanode in cluster_information[cluster_id]["datanode"]:
                f.write("./project/cmake/build/run_datanode "+str(each_datanode[0])+":"+str(each_datanode[1])+"\n")
            f.write("\n")
        for address in networkcore:
            f.write("./project/cmake/build/run_datanode "+address+"\n")
            f.write("\n")
        cnt = 0
        for proxy_ip_port in proxy_ip_list:
            f.write("./project/cmake/build/run_proxy "+str(proxy_ip_port[0])+":"+str(proxy_ip_port[1])+" "+networkcore_address+" "+coordinator_ip+"\n") 
            cnt += 1
            if cnt == cluster_number:
                break  
        f.write("\n")

def generater_cluster_information_xml():
    file_name = parent_path + '/project/config/clusterInformation.xml'
    import xml.etree.ElementTree as ET
    root = ET.Element('clusters')
    root.text = "\n\t"
    for cluster_id in cluster_information.keys():
        cluster = ET.SubElement(root, 'cluster', {'id': str(cluster_id), 'proxy': cluster_information[cluster_id]["proxy"]})
        cluster.text = "\n\t\t"
        datanodes = ET.SubElement(cluster, 'datanodes')
        datanodes.text = "\n\t\t\t"
        for index,each_datanode in enumerate(cluster_information[cluster_id]["datanode"]):
            datanode = ET.SubElement(datanodes, 'datanode', {'uri': str(each_datanode[0])+":"+str(each_datanode[1])})
            #datanode.text = '\n\t\t\t'
            if index == len(cluster_information[cluster_id]["datanode"]) - 1:
                datanode.tail = '\n\t\t'
            else:
                datanode.tail = '\n\t\t\t'
        datanodes.tail = '\n\t'
        if cluster_id == len(cluster_information)-1:
            cluster.tail = '\n'
        else:
            cluster.tail = '\n\t'
    #root.tail = '\n'
    tree = ET.ElementTree(root)
    tree.write(file_name, encoding="utf-8", xml_declaration=True)

            
def cluster_generate_run_proxy_datanode_file(i):
    file_name = parent_path + '/run_cluster_sh/' + str(i) +'/cluster_run_proxy_datanode.sh'
    with open(file_name, 'w') as f:
        f.write("pkill -9 run_datanode\n")
        f.write("pkill -9 run_proxy\n")
        f.write("\n")
        for j in range(vir_clu_per_pys_node):
            ip = proxy_ip_list[i][0]
            port = proxy_ip_list[i][1] + j * 50
            for k in range(datanode_number_per_cluster):
                datanode_port = datanode_port_start + i * 100 + j * 50 + k
                f.write("./project/cmake/build/run_datanode "+ip+":"+str(datanode_port)+"\n")
            f.write("\n") 
            f.write("./project/cmake/build/run_proxy "+ip+":"+str(port)+" "+networkcore_address+" "+coordinator_ip+"\n")   
            f.write("\n")

if __name__ == "__main__":
    generate_cluster_info_dict()
    generater_cluster_information_xml()
    if iftest:
        generate_run_proxy_datanode_file()
    else:
        cnt = 0
        for i in range(cluster_number):
            cluster_generate_run_proxy_datanode_file(cnt)
            cnt += 1
        for address in networkcore:
            file_name = parent_path + '/run_cluster_sh/' + str(cnt) +'/cluster_run_proxy_datanode.sh'
            with open(file_name, 'w') as f:
                f.write("./project/cmake/build/run_datanode "+address+"\n")
                f.write("\n")
            cnt += 1
    