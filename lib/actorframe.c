#include <stdlib.h>
#include <stdio.h>
#include "mpi.h"
#include "pool.h"

//---------------------------------------------------------------------------
// Macros
//---------------------------------------------------------------------------

#define MAX_ACTOR_CNT 200
#define MAX_ACTOR_TYPES 10
#define MAX_NODE_CNT 100
#define ACTOR_MSGQUEUE_LEN 100

#define AF_CONTROL_TAG 10000
#define AF_ACTORMSG_TAG 10001

//---------------------------------------------------------------------------
// Definitions
//---------------------------------------------------------------------------


//The actor states
typedef enum _ActorState { AS_Ready,
                           AS_Running,
                           AS_Suspended,
                           AS_Stopped,
                           AS_Cleaned } ActorState;

//The assign strategy of the actors: OnePerNode-One actor per node, Even-Assign the actors to nodes evenly
//                                   MaxInNode-Assign to one node till reach its limitation,then next node
//                                   Zero-Don't assign automatically, maybe assign manually later.
typedef enum _AssignMethod { AM_OnePerNode,
                             AM_Even,
                             AM_MaxInNode,
                             AM_Zero } AssignMethod;

typedef enum _FrameState { FS_New,
                           FS_Ready,
                           FS_Init,
                           FS_Running,
                           FS_Stopped } FrameState;

typedef enum _FrameMsgType { FM_ActorNew,
                             FM_ActorDie,
                             FM_ActorRun,
                             FM_ActorSuspend } FrameMsgType;

// Actor Message Structure
typedef struct _FrameMessage
{
    FrameMsgType msgType;
    int targetActor;           //And if the lower 16 bots is 0 , means this should be adopted on every actor in the node
    unsigned char data[32];
} FrameMessage;

typedef struct _ActorMessage
{
    int sndActorID; //If the lower 16 bits is 0, means it's sent by a node.
    int rcvActorID; //And if it's 0 , means a broadcast, every actor should process it.
    int msgCatogory;         //Defined by the user
    int bodyLength;
    unsigned char body[256];
} ActorMessage ;
 
// Actor Message Round Buffer
typedef struct _MsgRoundBuf
{
    int head;
    int tail;
    ActorMessage msg[ACTOR_MSGQUEUE_LEN];
} MsgRoundBuf ;

// Actor Structure
typedef struct _Actor
{
    unsigned actorID;  //Generated automatically Higher 16 bit - node number , lower 16 bit - actor number
    char category[10]; //The category of an actor, assigned by framework when create this actor.
    int categorySeq;   //The global sequece of a actor in the same category.
    long step;         //The current step number.
    ActorState state;
    MsgRoundBuf msgQueue; //The message round buffer queue that belong to the Actor.
    int (*Run)();
    int (*Init)(void *initData, int length);
    int (*Destroy)();
    void *pActorData;
    Actor *pNext;
} Actor;

// Actor description, used when config the framework
typedef struct _ActorDesc
{
    char category[10]; //The category of an actor, assigned by framework when create this actor.
    int (*Run)();
    int (*Init)();
    int (*Destroy)();
    int actorinNode;
    int actorCount; //The actor count when initialize.
    int nodelimit;  //How many nodes can be used in total, if this value < 0, means infinity.
    int actorlimit; //How many actors can be assigned in a node.
    int actorsBuilt;
    AssignMethod method;
} ActorDesc;

// Framework description, used when config the framework
typedef struct _FrameDesc
{
    int (*Initialize)();
    int (*Finalize)();
    int (*MasterRun)();
    int (*WorkerRun)();
} FrameDesc;

// Actor and node mapping table of the whole program.
typedef struct _NodeMappingTable
{
    int actorID;
    int node;
    char category[10];
    int categorySeq;
} NodeMappingTable;

//The handle of the actor
typedef struct _ActorFrameworkHandle
{
    // Actor queue, acturally is the place that save the actor's personal data and function pointer
    FrameState state;
    Actor *pActorQueue;
    NodeMappingTable mapTable[MAX_ACTOR_CNT];
    int mapTableCnt;
    int actortypes;
    FrameDesc frame;
    ActorDesc actorinfo[MAX_ACTOR_TYPES];
    int actorinNode[MAX_NODE_CNT];
    int activeNodeCnt;

    int rank;
    int ranksize;
} ActorFrameworkHandle;

//---------------------------------------------------------------------------
// Variables
//---------------------------------------------------------------------------
static ActorFrameworkHandle handleGlobal;


MPI_Request cntlRequest = NULL;
FrameMessage cntlMessage;

MPI_Request actorRequest = NULL;
ActorMessage actorMessage;

MPI_Request cntlBcastRequest = NULL;
FrameMessage cntlBcastMessage;

MPI_Request actorBcastRequest = NULL;
ActorMessage actorBcastMessage;

MPI_Status status;

static MPI_Datatype AF_CNTL_TYPE;
static MPI_Datatype AF_ACTOR_TYPE;



//---------------------------------------------------------------------------
// Functions
//---------------------------------------------------------------------------


void CreateFrameMsgType() {
	FrameMessage msg;
	MPI_Aint addr[3];
	MPI_Address(&msg.msgType, &addr[0]);
	MPI_Address(&msg.targetActor, &addr[1]);
	MPI_Address(&msg.data, &addr[2]);
    int blocklengths[3] = {1,1,16}, nitems=3;
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
    int blocklengths[5] = {1,1,1,1,64}, nitems=5;
	MPI_Datatype types[5] = {MPI_INT, MPI_INT, MPI_INT, MPI_INT, MPI_CHAR};
	MPI_Aint offsets[5] = {0, addr[1]-addr[0],addr[2]-addr[1],addr[3]-addr[2],addr[4]-addr[3]};
	MPI_Type_create_struct(nitems, blocklengths, offsets, types, &AF_ACTOR_TYPE);
	MPI_Type_commit(&AF_ACTOR_TYPE);
}



/** 
 * @brief Config the Actor Framework, includes：
 *          The strategy of actor assignment   One actor per node, or even mode/Multi actors one node, given the maximum actors per node and init nodes 
 *          The initialize count of actors in total
 *          The function pointer of framework should call when initialize and finalize.
 *        
 * @param index    参数1
 * @param t            参数2 @see CTest
 *
 * @return 返回说明
 *        -<em>false</em> fail
 *        -<em>true</em> succeed
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
 *        
 * @param index    参数1
 * @param t            参数2 @see CTest
 *
 * @return 返回说明
 *        -<em>false</em> fail
 *        -<em>true</em> succeed
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
                        (*pActor)->msgQueue.tail = -1;
                        (*pActor)->pNext = NULL;
                        (*pActor)->step = 0;
                        (*pActor)->state = AS_Ready;
                        pActor = &(*pActor)->pNext ;
                        (*pActor)->Init(NULL,0);                        
                    }
                    handleGlobal.mapTableCnt++;
                    handleGlobal.actorinNode[n]++;

                }
            }
        }
        else if (pDesc->method == AM_MaxInNode)
        {
            nodeCnt = pDesc->actorCount / pDesc->actorlimit + (pDesc->actorCount % pDesc->actorlimit) >0 ? 1:0;
            if (pDesc->nodelimit >= 0)  
            {
                if (nodeCnt < pDesc->nodelimit || nodeCnt < (handleGlobal.ranksize - 1))
                {
                    errMessage("No enough node for actors.");
                    DeleteHandle();
                    return 0;
                }
            }
            activeNodeCnt = activeNodeCnt > nodeCnt ? activeNodeCnt : nodeCnt;

            for (int n = 1; n <= nodeCnt; n++)
            {
                actorCnt = (n <= pDesc->actorCount / pDesc->actorlimit)? pDesc->actorlimit:pDesc->actorCount % pDesc->actorlimit;
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
                        (*pActor)->Init(NULL,0);                        
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
                    (*pActor)->Init(NULL,0);                        
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

	MPI_Comm_rank(MPI_COMM_WORLD, &handleGlobal.rank);
	MPI_Comm_size(MPI_COMM_WORLD, &handleGlobal.ranksize);

    //Build the maptable and the actor queue
    if (BuildMapTable() == 0)
    {
        MPI_Finalize();
        return 0;
    }
 
    int statusCode = processPoolInit();

    if (statusCode == 0)     
    {
        MPI_Finalize();
        return 0;
    }

    CreateFrameMsgType();
    CreateActorMsgType();

    if (handleGlobal.frame.Initialize() == 0)
    {
        MPI_Type_free(&AF_ACTOR_TYPE);
    	MPI_Type_free(&AF_CNTL_TYPE);
        MPI_Finalize();
        return 0;
    }
    handleGlobal.state = FS_Init;

    return statusCode;
}


typedef struct _ActorNew
{
    NodeMappingTable info;
    unsigned char data[16];
} ActorNew;

int GetAvailiableNode(ActorDesc *pDesc)
{
    int count;
    if (pDesc->method == AM_Even)
    {
        count = pDesc->actorlimit;
        for (int i=0 ; i<handleGlobal.activeNodeCnt ; i++) 
        {
            if (handleGlobal.actorinNode[])

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
    }

}


int ProcessFrameworkMessage(int rank, FrameMessage *pMsg)
{
    if (rank == 0)
    {
        switch (pMsg->msgType)
        {
            case FM_ActorNew:
                CreateActor(&pMsg->data);
        }
    }
    else
    {
        d
    }

    return 1;
}

Actor *SearchActor(int actorID)
{
    Actor *pActor = handleGlobal.pActorQueue;
    while (pActor == NULL)
    {
        if (pActor->actorID != actorID)     pActor = pActor->pNext;
        else break;
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
                pActor->msgQueue.msg[pActor->msgQueue.head] = *pMsg;
                pActor->msgQueue.head++;
                if (pActor->msgQueue.tail == -1)    pActor->msgQueue.tail=0;
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
        MPI_Irecv(&cntlBcastMessage, 1, AF_CNTL_TYPE, MPI_ANY_SOURCE, AF_CONTROL_TAG, MPI_COMM_WORLD, &cntlBcastRequest);

    MPI_Test(&cntlRequest, &flag, &status);
    if (flag)   
    {
        ProcessFrameworkMessage(handleGlobal.rank, &cntlMessage);
        MPI_Irecv(&cntlMessage, 1, AF_CNTL_TYPE, MPI_ANY_SOURCE, AF_CONTROL_TAG, MPI_COMM_WORLD, &cntlRequest);
    }
    MPI_Test(&cntlBcastRequest, &flag, &status);
    if (flag)   
    {
        ProcessFrameworkMessage(handleGlobal.rank, &cntlBcastMessage);
        MPI_Irecv(&cntlBcastMessage, 1, AF_CNTL_TYPE, MPI_ANY_SOURCE, AF_CONTROL_TAG, MPI_COMM_WORLD, &cntlBcastRequest);
    }

    if (actorRequest == NULL)
        MPI_Irecv(&actorMessage, 1, AF_ACTOR_TYPE, MPI_ANY_SOURCE, AF_ACTORMSG_TAG, MPI_COMM_WORLD, &actorRequest);
    if (cntlRequest == NULL)
        MPI_Irecv(&actorBcastMessage, 1, AF_ACTOR_TYPE, MPI_ANY_SOURCE, AF_ACTORMSG_TAG, MPI_COMM_WORLD, &actorBcastRequest);

    MPI_Test(&actorRequest, &flag, &status);
    while (flag)   
    {
        DispatchActorMessage(handleGlobal.rank, &actorMessage);
        MPI_Irecv(&actorMessage, 1, AF_ACTOR_TYPE, MPI_ANY_SOURCE, AF_CONTROL_TAG, MPI_COMM_WORLD, &actorRequest);
        MPI_Test(&actorRequest, &flag, &status);
    }
    MPI_Test(&actorBcastRequest, &flag, &status);
    while (flag)   
    {
        DispatchActorMessage(handleGlobal.rank, &actorBcastMessage);
        MPI_Irecv(&actorBcastMessage, 1, AF_ACTOR_TYPE, MPI_ANY_SOURCE, AF_CONTROL_TAG, MPI_COMM_WORLD, &actorBcastRequest);
        MPI_Test(&actorBcastRequest, &flag, &status);
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
            masterStatus = FrameworkPoll();

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

int BroadcastMessage()
{

    return 0;
}

int SendMessagetoActor()
{
    return 1;
}

int FinalizeActorFramwork()
{
    return 1;
}


