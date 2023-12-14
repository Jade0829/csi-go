from flask import Flask, request, jsonify
import json
import subprocess
import os
from threading import Thread

app = Flask(__name__)


class ThreadWithReturnValue(Thread):
    def __init__(self, group=None, target=None, name=None, args=(), kwargs=None, *, daemon=None):
        Thread.__init__(self,group,target,name,args,kwargs,daemon=daemon)

        self._return = None

    def run(self):
        if self._target is not None:
            self._return = self._target(*self._args, **self._kwargs)

    def join(self):
        Thread.join(self)
        return self._return

def getLVInfo():
    result = subprocess.getstatusoutput("lvs")
    result,err = result[1].split("\n")[1:],result[0]
    if err!=0:
        return err[1]

    lvList = []

    for i in result:
        lv = {}
        data = i.split()
        lv['name'] = data[0]
        lv['vgname'] = data[1]
        size,unit = float(data[3][:-1]),data[3][-1]

        if unit == "g":
            size = size*1024*1024
        elif unit == "t":
            size = size*1024*1024*1024

        lv['size'] = int(size)

        lvList.append(lv)


    return {"Result":lvList}

def getVGList():
    result = subprocess.getstatusoutput("vgs | grep storage")
    result, err = result[1].split("\n"),result[0]
    if err!=0:
        return {"Result":err[1]}

    vgList = []

    for i in result:
        vg = {}
        data = i.split()
        vg['Name'] = data[0]

        totalUnit, freeUnit = data[-2][-1], data[-1][-1]
        totalSize, freeSize = data[-2][:-1],data[-1][1:-1]

        if totalSize[0] == '<':
            totalSize = totalSize[1:]
        if freeSize[0] == '<':
            freeSize = freeSize[1:]

        totalSize = float(totalSize)
        freeSize = float(freeSize)


        if totalUnit == "g":
            totalSize *= 1024
        elif totalUnit == "t":
            totalSize *= 1024*1024

        if freeUnit == "g":
            freeSize *= 1024
        elif freeUnit == "t":
            freeSize *= 1024*1024
         

        vg['TotalSizeMiB'] = int(totalSize)
        vg['FreeSizeMiB'] = int(freeSize)

        vgList.append(vg)
    print({"Result":vgList})

    return {"Result":vgList}

def getFuse():
    cmd = "df -T | grep fuse | grep -v 'private'"
    err, res = subprocess.getstatusoutput(cmd)

    if err != 0:
        return {"Result" : err[1]}

    res = res.split("\n")

    fuses = []

    for i in res:
        fuse = {}
        data = i.split()

        fuse['TotalSizeMiB'] = int(int(data[2]) / 1024)
        fuse['FreeSizeMib'] = int(int(data[4]) / 1024)
        fuse['Name'] = data[-1]

        fuses.append(fuse)

    print(fuse)
    return {"Result":fuses}

def CreateFuse(size,lvol_name,path):
    fileName = path+"/"+lvol_name

    size = size

    cmd = "xfs_mkfile %dm %s"%(size,fileName)
    err, msg = subprocess.getstatusoutput(cmd)

    if err != 0:
        return 1, msg

    return 0, fileName

def CreateLV(size,model,vgname):
    cmd = "lvcreate -i 2 -n %s -L %s %s"%(model,size,vgname)
    err, msg = subprocess.getstatusoutput(cmd)

    if err != 0:
        return 1, msg

    device = "/dev/%s/%s"%(vgname,model)

    return 0, device

def GetNVMeTargetInfo():
    defaultPath = "/csi-proxy/nvmeTargetInfo.json"
    nvmeInfo = {}

    if not os.path.isfile(defaultPath):
        nvmeInfo["data"]=[]
    else:
        with open(defaultPath,"r",encoding="UTF-8") as f:
            ctx = f.read()
            if not ctx:
                nvmeInfo["data"]=[]
            else:
                nvmeInfo = json.loads(ctx)

    return nvmeInfo



def CreateSubsystem(model,sn,nqn,size,device):
    defaultPath = "/sys/kernel/config/nvmet/subsystems/"
    fName = ["attr_model","attr_serial","attr_allow_any_host","device_path","enable"]

    err = subprocess.getstatusoutput("mkdir %s%s"%(defaultPath,nqn))
    uuid = ""

    if err[0]!=0:
        return 1,err[1],nqn,device,uuid

    if os.path.isfile(defaultPath+nqn+"/"+fName[0]):
        err = subprocess.getstatusoutput("echo %s > %s"%(model,defaultPath+nqn+"/"+fName[0]))
        if err[0]!=0:
            return 1,err[1],nqn,device,uuid

    if os.path.isfile(defaultPath+nqn+"/"+fName[1]):
        err = subprocess.getstatusoutput("echo %s > %s"%(sn,defaultPath+nqn+"/"+fName[1]))
        if err[0]!=0:
            return 1,err[1],nqn,device,uuid

    if os.path.isfile(defaultPath+nqn+"/"+fName[2]):
        err = subprocess.getstatusoutput("echo 1 > %s"%defaultPath+nqn+"/"+fName[2])
        if err[0]!=0:
            return 1,err[1],nqn,device,uuid

    nsPath = defaultPath+nqn+"/namespaces/"
    ns = len(os.listdir(nsPath))+1
    nsPath += str(ns)

    err = subprocess.getstatusoutput("mkdir %s"%(nsPath))
    if err[0]!=0:
        return 1,err[1],nqn,device,uuid

    err = subprocess.getstatusoutput("echo %s > %s"%(device,nsPath+"/"+fName[3]))
    if err[0]!=0:
        return 1,err[1],nqn,device,uuid

    err = subprocess.getstatusoutput("echo 1 > %s"%(nsPath+"/"+fName[4]))
    if err[0]!=0:
        return 1,err[1],nqn,device,uuid

    err, uuid = subprocess.getstatusoutput("cat %s"%(nsPath+"/device_uuid"))

    print(f"UUID : {uuid}")
    if err !=0:
        return 1,err,nqn,device,uuid

    return 0,defaultPath+nqn,nqn,device,uuid

def CreateTCP(subsystemPath,ip,port):
    defaultPath = "/sys/kernel/config/nvmet/ports/1"
    fName = ["addr_adrfam","addr_trtype","addr_traddr","addr_trsvcid"]


    err = subprocess.getstatusoutput("ln -s %s %s"%(subsystemPath,defaultPath+"/subsystems/"))
    if err[0]!=0:
        return 1,err[1]

    return 0,defaultPath

def SaveNVMeTargetInfo(ctx,ip,port,uuid,nqn,model,sn,device,tcpPath):
    data = {}
    file_path = "/csi-proxy/nvmeTargetInfo.json"

    with open(file_path,"w",encoding="UTF-8") as f:
        data["device"]=device
        data["model"]=model
        data["sn"]=sn
        data["nqn"]=nqn
        data["tcppath"]=tcpPath
        ctx["data"].append(data)
        print(ctx)
        json.dump(ctx,f)

def deleteTarget(tcpPath,nqn):
    defaultPath = "/sys/kernel/config/nvmet/"
    subsystemPath = defaultPath+"subsystems/"+nqn

    print(tcpPath)
    print(subsystemPath)

    err = subprocess.getstatusoutput("rm -f %s"%(tcpPath+"/subsystems/"+nqn))
    if err[0]!=0:
        return 1, err[1]

    err = subprocess.getstatusoutput("rmdir %s"%(subsystemPath+"/namespaces/*"))
    if err[0]!=0:
        return 1, err[1]

    err = subprocess.getstatusoutput("rmdir %s"%(subsystemPath))
    if err[0]!=0:
        return 1, err[1]

    return 0,"Deleted NVMe-oF Target\n"

def deleteLV(nqn):
    data = {}
    path = "/csi-proxy/nvmeTargetInfo.json"
    tcpPath = ""

    print("Delete Start\n")


    with open(path,"r",encoding="UTF-8") as f:
        ctx = f.read()
        data = json.loads(ctx)

        cnt = 0
        for d in data["data"]:
            if nqn == d["nqn"]:
                tcpPath = d["tcppath"]

                err, ctx=deleteTarget(tcpPath,nqn)
                if err:
                    return "error: "+ctx

                cmd = ""

                if lvType == "fuse":
                    cmd = "rm -f %s"%d["device"]
                else:
                    cmd = "lvremove %s -y"%d["device"][5:]

                res = subprocess.getstatusoutput(cmd)
                if res[0]!=0:
                    return "error: "+res[1]

                del data["data"][cnt]

                data = data
                break

            cnt += 1

    with open(path,"w",encoding="UTF-8") as f:
        json.dump(data,f)


    result = {"Result":"Delete Success\n"}
    return jsonify(result)

def create(lvolID,size,lvname):
    global ip
    global port 

    device = ""

    if lvType == "fuse":
        err, device = CreateFuse(size,lvolID,lvname)
    if lvType == "lvm":
        err, device = CreateLV(size,lvolID,lvname)

    model = lvolID
    nqn = "nqn.gluesys.csi:%s"%lvolID
    sn = "Gluesys-S1"

    print(device)

    err, path, nqn, device, uuid = CreateSubsystem(model,sn,nqn,size,device)
    if err != 1:
        print(path)
        err, tcpPath = CreateTCP(path,ip,port)
        if err != 1:
            SaveNVMeTargetInfo(GetNVMeTargetInfo(),ip,port,uuid,nqn,model,sn,device,tcpPath)
        else:
            print(tcpPath)

    return uuid

@app.route("/createTarget",methods=['POST','GET'])
def createTarget():
    if request:
        data = request.get_json()


        lvolID = data["params"]["lvol_name"]
        size = data["params"]["size"]
        lvname = data["params"]["lv_name"]


        th1 = ThreadWithReturnValue(target=create,args=(lvolID,size,lvname))

        th1.start()

        uuid = th1.join()


        result = {"Result":{"LvolID":lvolID,"UuId":uuid}}

        print(result)

        return jsonify(result)

@app.route("/deleteTarget",methods=['POST','GET'])
def delete():
    if request:
        data = request.get_json()
        print(data.keys())

        nqn = "nqn.gluesys.csi:"+data["params"]["name"]


        return deleteLV(nqn)

@app.route("/getVolume",methods=['POST','GET'])
def getVolume():
    global lvType
    if request:

        if lvType == "lvm":
            vgl = getVGList()
            return vgl
        if lvType == "fuse":
            return getFuse()

if __name__ == "__main__":
    global lvType
    global ip
    global port

    cmd = "hostname -i"
    res = subprocess.getstatusoutput(cmd)[1]

    res = res.replace("\n","")

    ip = res.replace(" ","")
    port = 4420

    lvType = "lvm"
    app.run(host=ip,port=829)
















