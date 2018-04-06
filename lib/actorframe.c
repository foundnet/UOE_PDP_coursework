#include <stdlib.h>
#include <stdio.h>
#include "mpi.h"
#include "pool.h"
#include "actorframe.h"

//---------------------------------------------------------------------------
// Functions
//---------------------------------------------------------------------------


void CreateFrameMsgType() {
	FrameMessage msg;
	MPI_Aint addr[3];
	MPI_Address(&msg.msgType, &addr[0]);
	MPI_Address(&msg.targetActor, &addr[1]);
	MPI_Address(&msg.data, &addr[2]);
    int blocklengths[3] = {1,1,64}, nitems=3;
	MPI_Datatype types[3] = {MPI_INT, MPI_INT, MPI_CHAR};
	MPI_Aint offsets[3] = {0, addr[1]-addr[0],addr[2]-addr[1]};
	MPI_Type_create_struct(nitems, blocklengths, offsets, types, &AF_CNTL_TYPE);
	MPI_Type_commit(&AF_CNTL_TYPE);
}

void CreateActorMsgType() {
	ActorMessage msg;
	MPI_Aint addr[5];
	MPI_Address(&msg.sndActorID, &addr[0]);
	MPI_Address(&msg.rcvActorID, &addr[1]);
	MPI_Address(&msg.msgCatogory, &addr[2]);
	MPI_Address(&msg.bodyLength, &addr[3]);
    MPI_Address(&msg.body, &addr[4]);
    int blocklengths[5] = {1,1,1,1,256}, nitems=5;
	MPI_Datatype types[5] = {MPI_INT, MPI_INT, MPI_INT, MPI_INT, MPI_CHAR};
	MPI_Aint offsets[5] = {0, addr[1]-addr[0],addr[2]-addr[1],addr[3]-addr[2],addr[4]-addr[3]};
	MPI_Type_create_struct(nitems, blocklengths, offsets, types, &AF_ACTOR_TYPE);
	MPI_Type_commit(&AF_ACTOR_TYPE);
}



int BroadcastCntlMessage(FrameMsgType msgType, int actor, void *pData, int dataLen)
{
    if (handleGlobal.rank == 0 && dataLen <= 32 && dataLen >=0)
    {
        FrameMessage msg;
        memset (msg.data, 0, 32);
        msg.msgType = msgType;
        msg.targetActor = actor;
        memcpy (msg.data, pData, dataLen);

        MPI_Request request;
        MPI_Ibcast(&msg, 1, AF_CNTL_TYPE, 0, MPI_COMM_WORLD, &request);

        return 1;
    }

    return 0;
}


int SendActorMessage(Actor *this, int destID, int msgType, void *pData, int dataLen)
{
    int node = destID >> 16;
    if (handleGlobal.rank != node && dataLen <= 128 && dataLen >=0)
    {
        ActorMessage msg;
        memset (msg.body, 0, 128);
        msg.msgCatogory = msgType;
        msg.bodyLength = dataLen;
        msg.rcvActorID = destID;
        msg.sndActorID = this->actorID;
        memcpy (msg.body, pData, dataLen);

        MPI_Request request;
        int node = destID >> 16;

        MPI_Send(&msg,1,AF_ACTOR_TYPE, node , AF_ACTORMSG_TAG,MPI_COMM_WORLD);
    }    
    return 1;
}



int SendCntlMessage(FrameMsgType msgType, int node, int actor, void *pData, int dataLen)
{
    if (handleGlobal.rank != node && dataLen <= 32 && dataLen >=0)
    {
        FrameMessage msg;
        memset (msg.data, 0, 32);
        msg.msgType = msgType;
        msg.targetActor = actor;
        memcpy (msg.data, pData, 32);

        MPI_Request request;

        MPI_Bsend(&msg,1,AF_CNTL_TYPE,node,AF_CONTROL_TAG,MPI_COMM_WORLD);
    }    
    return 1;
}

int ReadActorMessage(Actor *actor, ActorMessage *outMsg)
{
    if (actor != NULL)
    {
        if (actor->msgQueue.tail != actor->msgQueue.head)
        {
            memcpy(outMsg, &actor->msgQueue.msg[actor->msgQueue.tail],sizeof(ActorMessage));
            int point = (actor->msgQueue.tail + 1) % ACTOR_MSGQUEUE_LEN;
            actor->msgQueue.tail = point;
            return 1;
        }
    }
    return 0;
}

int FinalizeActorFramwork()
{
    return 1;
}


/** 
 * @brief Config the Actor Framework, includes：
 *          The strategy of actor assignment   One actor per node, or even mode/Multi actors one node, given the maximum actors per node and init nodes 
 *          The initialize count of actors in total
 *          The function pointer of framework should call when initialize and finalize.
*/
int ConfigActorFrame(FrameDesc desc)
{
    if (handleGlobal.state < FS_Running)
    {
        handleGlobal.frame = desc;

        handleGlobal.state = FS_Ready;
        return 1;
    }

    return 0;
}

/** 
 * @brief Add the actor's profile, including the actor's type, personal data, strategy, function pointer, etc.
 *        The function pointer includes the init 
*/
int AddActorProfile(ActorDesc desc)
{
    if (handleGlobal.state < FS_Running &&
        handleGlobal.actortypes < 9)
    {
        handleGlobal.actorinfo[handleGlobal.actortypes] = desc;
        handleGlobal.actortypes++;

        return 1;
    }

    return 0;
}

void DeleteHandle()
{
    Actor *pActor = handleGlobal.pActorQueue;
    while(pActor != NULL)
    {
        Actor *pNext = pActor->pNext;
        if (pActor->pActorData != NULL) free(pActor->pActorData);
        free(pActor);
        pActor = pNext;
    }
}

// Build the mapTable and create actor queue, if faied, return 0 , or return the count of the active nodes.
int BuildMapTable()
{
    memset(handleGlobal.mapTable, 0, sizeof(NodeMappingTable) * MAX_ACTOR_CNT);
    handleGlobal.mapTableCnt = 0;

    memset(handleGlobal.actorinNode,0,sizeof(int)*MAX_ACTOR_CNT);
    handleGlobal.activeNodeCnt = 0;

    handleGlobal.pActorQueue = NULL;
    Actor **pActor = &handleGlobal.pActorQueue;

    int activeNodeCnt = 0;
    int totalActor = 0;
    int nodeCnt,actorCnt = 0; // The actor count per node, and the node count, used temporarily

    for (int i = 0; i < handleGlobal.actortypes; i++)
    {
        ActorDesc *pDesc = &handleGlobal.actorinfo[i];
        pDesc->actorsBuilt = 0;

        if (pDesc->method == AM_Even)
        {
            if (pDesc->nodelimit >= 0)  nodeCnt = (pDesc->actorCount >= pDesc->nodelimit) ? pDesc->nodelimit : pDesc->actorCount;
            else
                nodeCnt = 0;
            nodeCnt = nodeCnt > (handleGlobal.ranksize - 1) ? (handleGlobal.ranksize - 1) : nodeCnt;
            if (pDesc->actorlimit < pDesc->actorCount / nodeCnt)    
            {
                errMessage("No enough node for actors.");
                DeleteHandle();
                return 0;
            }
            
            activeNodeCnt = activeNodeCnt > nodeCnt ? activeNodeCnt : nodeCnt;

            for (int n = 1; n <= nodeCnt; n++)
            {
                actorCnt = pDesc->actorCount / nodeCnt;
                actorCnt += (pDesc->actorCount % nodeCnt >= n) ? 1 : 0;
                //Build the maptable
                for (int m = 0; m < actorCnt; m++)
                {
                    pDesc->actorsBuilt++;
                    handleGlobal.mapTable[handleGlobal.mapTableCnt].actorID = n << 16 + handleGlobal.mapTableCnt + 1;
                    memcpy(handleGlobal.mapTable[handleGlobal.mapTableCnt].category, pDesc->category, 10);
                    handleGlobal.mapTable[handleGlobal.mapTableCnt].categorySeq = pDesc->actorsBuilt;
                    handleGlobal.mapTable[handleGlobal.mapTableCnt].node = n;

                    // Build the actor queue.
                    if (n == handleGlobal.rank) 
                    {
                        (*pActor) = (Actor*)malloc(sizeof(Actor));
                        (*pActor)->actorID = handleGlobal.mapTable[handleGlobal.mapTableCnt].actorID;
                        (*pActor)->categorySeq = handleGlobal.mapTable[handleGlobal.mapTableCnt].categorySeq;
                        memcpy((*pActor)->category,handleGlobal.mapTable[handleGlobal.mapTableCnt].category, 10);
                        (*pActor)->pActorData = NULL;
                        (*pActor)->msgQueue.head = 0;
                        (*pActor)->msgQueue.tail = 0;
                        (*pActor)->pNext = NULL;
                        (*pActor)->step = 0;
                        (*pActor)->state = AS_Ready;
                        pActor = &(*pActor)->pNext ;
                        (*pActor)->Init((*pActor),NULL,0);                        
                    }
                    handleGlobal.mapTableCnt++;
                    handleGlobal.actorinNode[n]++;

                }
            }
        }
        else if (pDesc->method == AM_OnePerNode)
        {
            if (pDesc->actorCount > pDesc->nodelimit)
            {
                errMessage("The count of actors is excced the node limits.");
                DeleteHandle();
                return 0;
            } else if (pDesc->actorCount > (handleGlobal.ranksize - 1) )
            {
                errMessage("No enough node for actors.");
                DeleteHandle();
                return 0;
            } else
                nodeCnt = pDesc->actorCount;

            activeNodeCnt = activeNodeCnt > nodeCnt ? activeNodeCnt : nodeCnt;

            for (int n = 1; n <= nodeCnt; n++)
            {
                //Build the maptable
                pDesc->actorsBuilt++;
                handleGlobal.mapTable[handleGlobal.mapTableCnt].actorID = n << 16 + handleGlobal.mapTableCnt + 1;
                memcpy(handleGlobal.mapTable[handleGlobal.mapTableCnt].category, pDesc->category, 10);
                handleGlobal.mapTable[handleGlobal.mapTableCnt].categorySeq = pDesc->actorsBuilt;
                handleGlobal.mapTable[handleGlobal.mapTableCnt].node = n;

                // Build the actor queue.
                if (n == handleGlobal.rank)
                {
                    (*pActor) = (Actor *)malloc(sizeof(Actor));
                    (*pActor)->actorID = handleGlobal.mapTable[handleGlobal.mapTableCnt].actorID;
                    (*pActor)->categorySeq = handleGlobal.mapTable[handleGlobal.mapTableCnt].categorySeq;
                    memcpy((*pActor)->category, handleGlobal.mapTable[handleGlobal.mapTableCnt].category, 10);
                    (*pActor)->pActorData = NULL;
                    (*pActor)->msgQueue.head = 0;
                    (*pActor)->msgQueue.tail = 0;
                    (*pActor)->pNext = NULL;
                    (*pActor)->step = 0;
                    (*pActor)->state = AS_Ready;
                    pActor = &(*pActor)->pNext;
                    (*pActor)->Init((*pActor),NULL,0);                        
                }
                handleGlobal.mapTableCnt++;
                handleGlobal.actorinNode[n]++;
            }
        }
    }

    handleGlobal.activeNodeCnt = activeNodeCnt;

    return 1;

}

int InitActorFramework(int argc, char *argv[])
{
    if (handleGlobal.state != FS_Ready)     return 0;    
    //Init Framework
 
    // Call MPI initialize first
    MPI_Init(&argc, &argv);
    handleGlobal.commBuffer = malloc(512*1024);

    MPI_Buffer_attach(handleGlobal.commBuffer, 512*1024);

	MPI_Comm_rank(MPI_COMM_WORLD, &handleGlobal.rank);
	MPI_Comm_size(MPI_COMM_WORLD, &handleGlobal.ranksize);

    //Build the maptable and the actor queue
    if (BuildMapTable() == 0)
    {
        handleGlobal.commBuffer = malloc(512*1024);
        MPI_Finalize();
        return 0;
    }
 
    int statusCode = processPoolInit();

    if (statusCode == 0)     
    {
        handleGlobal.commBuffer = malloc(512*1024);
        MPI_Finalize();
        return 0;
    }

    CreateFrameMsgType();
    CreateActorMsgType();

    if (handleGlobal.frame.Initialize() == 0)
    {
        MPI_Type_free(&AF_ACTOR_TYPE);
    	MPI_Type_free(&AF_CNTL_TYPE);
        handleGlobal.commBuffer = malloc(512*1024);
        MPI_Finalize();
        return 0;
    }
    handleGlobal.state = FS_Init;

    return statusCode;
}



int GetAvailiableNode(ActorDesc *pDesc)
{
    int count, selnode=0;
    if (pDesc->method == AM_Even)
    {
        count = pDesc->actorlimit;
        for (int i=1 ; i<=handleGlobal.activeNodeCnt ; i++)
        {
            if (handleGlobal.actorinNode[i] < count)
            {
                count = handleGlobal.actorinNode[i];
                selnode = i;
            }
        }
        if (selnode == 0)
        {
            if (handleGlobal.activeNodeCnt < (handleGlobal.ranksize - 1))
                selnode = -1;
        }
    }
    else if (pDesc->method == AM_OnePerNode)
    {
        for (int i=1 ; i<=handleGlobal.activeNodeCnt ; i++)
        {
            if (handleGlobal.actorinNode[i] == 0)
                selnode = i;
        }
        if (selnode == 0)
        {
            if (handleGlobal.activeNodeCnt < (handleGlobal.ranksize - 1))
                selnode = -1;
        }
    }

    return selnode;

}

NodeMappingTable *AddMapTable(int node, ActorDesc *pDesc)
{
    NodeMappingTable *pMap;
    pDesc->actorsBuilt++;
    handleGlobal.mapTable[handleGlobal.mapTableCnt].actorID = node << 16 + handleGlobal.mapTableCnt + 1;
    memcpy(handleGlobal.mapTable[handleGlobal.mapTableCnt].category, pDesc->category, 10);
    handleGlobal.mapTable[handleGlobal.mapTableCnt].categorySeq = pDesc->actorsBuilt;
    handleGlobal.mapTable[handleGlobal.mapTableCnt].node = node;

    handleGlobal.mapTableCnt++;
    handleGlobal.actorinNode[node]++;

    pMap = &handleGlobal.mapTable[handleGlobal.mapTableCnt];

    return pMap;
}

void AppendMappingTable(NodeMappingTable *pMap, ActorDesc *pDesc)
{
    pDesc->actorsBuilt++;
    handleGlobal.mapTable[handleGlobal.mapTableCnt].actorID = pMap->actorID;
    memcpy(handleGlobal.mapTable[handleGlobal.mapTableCnt].category, pMap->category, 10);
    handleGlobal.mapTable[handleGlobal.mapTableCnt].categorySeq = pMap->categorySeq;
    handleGlobal.mapTable[handleGlobal.mapTableCnt].node = pMap->node;

    handleGlobal.mapTableCnt++;
    handleGlobal.actorinNode[pMap->node]++;
}

void RemoveMappingTable(int actorID)
{
    for (int i =0 ; i< handleGlobal.mapTableCnt; i++)
    {
        if (handleGlobal.mapTable[i].actorID == actorID)
        {
            handleGlobal.mapTable[i].actorID = 0;
            handleGlobal.actorinNode[handleGlobal.mapTable[i].node]--;
        }
    }
}


int CreateActor(ActorNew *pnewActor)
{
    //Get the target node
    ActorDesc *pDesc = NULL;
    for (int i=0 ; i< handleGlobal.actortypes ; i++)
        if (memcmp(handleGlobal.actorinfo[i].category, pnewActor->info.category, 10) == 0)
            pDesc = &handleGlobal.actorinfo[i];
    if (pDesc != NULL)
    {
        if (handleGlobal.rank == 0)
        {
            int node = pnewActor->info.node ;
            if (node == 0)
            {
                node = GetAvailiableNode(pDesc);
                if (node == -1)
                {
                    node = startWorkerProcess();
                }
            }
            if (node > 0)
            {
                NodeMappingTable *pMap = AddMapTable(node, pDesc);
                ActorNew data;
                data.info = *pMap;
                memcpy(data.data, pnewActor->data, 16);
                int result = BroadcastCntlMessage(FM_ActorNew, 0, &data, sizeof(ActorNew));
            }
        }  
        else
        {
            AppendMappingTable(&pnewActor->info, pDesc);
            if (pnewActor->info.node == handleGlobal.rank)
            {
                Actor **pActor = &handleGlobal.pActorQueue;
                while ((*pActor) != NULL)
                    pActor = &(*pActor)->pNext;

                (*pActor) = (Actor *)malloc(sizeof(Actor));
                (*pActor)->actorID = pnewActor->info.actorID;
                (*pActor)->categorySeq = pnewActor->info.categorySeq;
                memcpy((*pActor)->category, pnewActor->info.category, 10);
                (*pActor)->pActorData = NULL;
                (*pActor)->msgQueue.head = 0;
                (*pActor)->msgQueue.tail = 0;
                (*pActor)->pNext = NULL;
                (*pActor)->step = 0;
                pActor = &(*pActor)->pNext;
                (*pActor)->Init((*pActor),pnewActor->data, 16);
                (*pActor)->state = AS_Running;
            }
            return 1;
        }
    }
    return 0;
}

int ProcessFrameworkMessage(int rank, FrameMessage *pMsg)
{
    if (rank == 0)
    {
        switch (pMsg->msgType)
        {
        case FM_ActorNew:
            CreateActor((ActorNew *)&pMsg->data);
            break;
        case FM_ActorDie:
            RemoveMappingTable(pMsg->targetActor);
            BroadcastCntlMessage(FM_ActorDie, pMsg->targetActor,NULL,0 ) ;
        }
    }
    else
    {
        switch (pMsg->msgType)
        {
        case FM_ActorNew:
            CreateActor((ActorNew *)&pMsg->data);
            break;
        case FM_ActorRun:
            if (pMsg->targetActor == 0 || pMsg->targetActor >> 16 == handleGlobal.rank)
            {
                Actor **pActor = &handleGlobal.pActorQueue;
                while ((*pActor) != NULL)
                {
                    (*pActor)->state = AS_Running;
                    pActor = &(*pActor)->pNext;
 
                }
            }
            else 
            {
                Actor **pActor = &handleGlobal.pActorQueue;
                while ((*pActor) != NULL && pMsg->targetActor != (*pActor)->actorID)
                {
                    pActor = &(*pActor)->pNext;
                }
                if ((*pActor) != NULL)  (*pActor)->state = AS_Running;
            }
        case FM_ActorDie:
            if (pMsg->targetActor >> 16 == handleGlobal.rank)
            {
                Actor **pActor = &handleGlobal.pActorQueue;
                while ((*pActor) != NULL && pMsg->targetActor != (*pActor)->actorID)
                {
                    pActor = &(*pActor)->pNext;
                }
                if ((*pActor) != NULL)
                {
                    (*pActor)->state = AS_Stopped;
                    if ((*pActor)->pActorData != NULL)  free((*pActor)->pActorData);
                    Actor *pDel = (*pActor);
                    (*pActor) = (*pActor)->pNext;
                    free(pDel); 
                } 
            } 
            RemoveMappingTable(pMsg->targetActor);         
        }
    }

    return 1;
}

Actor *SearchActor(int actorID)
{
    Actor *pActor = handleGlobal.pActorQueue;
    while (pActor != NULL)
    {
        if (pActor->actorID != actorID)     pActor = pActor->pNext;
        else
            break;
    }

    return pActor;
}

int DispatchActorMessage(int rank, ActorMessage *pMsg)
{
    Actor *pActor;
    if (rank != 0)      // If this is worker node
    {
        if ((pMsg->rcvActorID >> 16) == rank) 
        {
            pActor = SearchActor(pMsg->rcvActorID);
            if (pActor != NULL)
            {
                int point = (pActor->msgQueue.head + 1) % ACTOR_MSGQUEUE_LEN;
                if (point != pActor->msgQueue.tail) 
                {
                    pActor->msgQueue.msg[point] = *pMsg;
                    pActor->msgQueue.head = point;
                }
                return 1;
            }
        }
    }
    return 0;
}

int FrameworkPoll()
{
    int flag;
    MPI_Status status;
    if (cntlRequest == NULL)
        MPI_Irecv(&cntlMessage, 1, AF_CNTL_TYPE, MPI_ANY_SOURCE, AF_CONTROL_TAG, MPI_COMM_WORLD, &cntlRequest);
    if (cntlBcastRequest == NULL)
        MPI_Ibcast(&cntlBcastMessage, 1, AF_CNTL_TYPE, 0, MPI_COMM_WORLD, &cntlBcastRequest);

    MPI_Test(&cntlRequest, &flag, &status);
    while (flag)   
    {
        ProcessFrameworkMessage(handleGlobal.rank, &cntlMessage);
        MPI_Irecv(&cntlMessage, 1, AF_CNTL_TYPE, MPI_ANY_SOURCE, AF_CONTROL_TAG, MPI_COMM_WORLD, &cntlRequest);
        MPI_Test(&cntlRequest, &flag, &status);
    }
    MPI_Test(&cntlBcastRequest, &flag, &status);
    while (flag)   
    {
        ProcessFrameworkMessage(handleGlobal.rank, &cntlBcastMessage);
        MPI_Ibcast(&cntlBcastMessage, 1, AF_CNTL_TYPE, 0, MPI_COMM_WORLD, &cntlBcastRequest);
        MPI_Test(&cntlBcastRequest, &flag, &status);
    }

    if (actorRequest == NULL)
        MPI_Irecv(&actorMessage, 1, AF_ACTOR_TYPE, MPI_ANY_SOURCE, AF_ACTORMSG_TAG, MPI_COMM_WORLD, &actorRequest);
//    if (cntlRequest == NULL)
//        MPI_Irecv(&actorBcastMessage, 1, AF_ACTOR_TYPE, MPI_ANY_SOURCE, AF_ACTORMSG_TAG, MPI_COMM_WORLD, &actorBcastRequest);

    MPI_Test(&actorRequest, &flag, &status);
    while (flag)   
    {
        DispatchActorMessage(handleGlobal.rank, &actorMessage);
        MPI_Irecv(&actorMessage, 1, AF_ACTOR_TYPE, MPI_ANY_SOURCE, AF_CONTROL_TAG, MPI_COMM_WORLD, &actorRequest);
        MPI_Test(&actorRequest, &flag, &status);
    }
//    MPI_Test(&actorBcastRequest, &flag, &status);
//    while (flag)   
//    {
//        DispatchActorMessage(handleGlobal.rank, &actorBcastMessage);
//        MPI_Irecv(&actorBcastMessage, 1, AF_ACTOR_TYPE, MPI_ANY_SOURCE, AF_CONTROL_TAG, MPI_COMM_WORLD, &actorBcastRequest);
//        MPI_Test(&actorBcastRequest, &flag, &status);
//    }
    return 1;
}


int ActorRun()
{
    Actor **pActor = &handleGlobal.pActorQueue;
    while ((*pActor) != NULL)
    {
        if ((*pActor)->state == AS_Running)   
        {
             (*pActor)->Run(*pActor);
             (*pActor)->step ++;
        }
        if ((*pActor)->state == AS_Stopped)
        {
            if ((*pActor)->pActorData != NULL)     free((*pActor)->pActorData);
            RemoveMappingTable((*pActor)->actorID);

            Actor *pDel = (*pActor);
            (*pActor) = (*pActor)->pNext;
            SendCntlMessage(FM_ActorDie, 0, pDel->actorID, NULL , 0 ) ;
            free(pDel);
        }
    }
}

int RunFramework()
{
    if (handleGlobal.state != FS_Init)
        return 0;

    if (handleGlobal.rank != 0)
    {
        // A worker so do the worker tasks
        workerCode();
    }
    else if (handleGlobal.rank > 0)
    {
        /*
		 * This is the master, each call to master poll will block until a message is received and then will handle it and return
         * 1 to continue polling and running the pool and 0 to quit.
		 */
        int i, activeWorkers = 0, returnCode;
        MPI_Request *pInitWorkerRequests = (MPI_Request*)malloc(sizeof(MPI_Request)*handleGlobal.ranksize);
		for (i=0; i<handleGlobal.activeNodeCnt; i++)    {
            int workerPid = startWorkerProcess();
            MPI_Irecv(NULL, 0, MPI_INT, workerPid, 0, MPI_COMM_WORLD, (pInitWorkerRequests+i));
            activeWorkers++;
            printf("[AF_MASTER]Master started worker %d on MPI process %d\n", i, workerPid);
		}
		int masterStatus = masterPoll();
		while (masterStatus) {
            masterStatus = masterPoll();
            int result = FrameworkPoll();
            result = ActorRun();

            for (i = 0; i < handleGlobal.activeNodeCnt; i++)
            {
                // Checks all outstanding workers that master spawned to see if they have completed
                if (*(pInitWorkerRequests+i) != MPI_REQUEST_NULL)
                {
                    MPI_Test((pInitWorkerRequests+i), &returnCode, MPI_STATUS_IGNORE);
                    if (returnCode)
                        activeWorkers--;
                }
            }

       
            // If we have no more active workers then quit poll loop which will effectively shut the pool down when  processPoolFinalise is called
            if (activeWorkers == 0)
                break;
		}
    }
    // Finalizes the process pool, call this before closing down MPI
    processPoolFinalise();
    // Finalize MPI, ensure you have closed the process pool first
    MPI_Finalize();
}
