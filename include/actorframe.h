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
                             AM_Zero } AssignMethod;

typedef enum _FrameState { FS_New,
                           FS_Ready,
                           FS_Init,
                           FS_Running,
                           FS_Stopped } FrameState;

typedef enum _FrameMsgType { FM_ActorNew,
                             FM_ActorDie,
                             FM_ActorRun,
                             FM_ActorSuspend,
                             FM_Custom } FrameMsgType;

// Actor Message Structure
typedef struct _FrameMessage
{
    FrameMsgType msgType;
    int targetActor;           //And if the lower 16 bots is 0 , means this should be adopted on every actor in the node
    unsigned char data[64];
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
    int (*Run)(Actor *actor);
    int (*Init)(Actor *actor, void *data, int len);
    int (*Destroy)(Actor *actor);
    void *pActorData;
    Actor *pNext;
} Actor;

// Actor description, used when config the framework
typedef struct _ActorDesc
{
    char category[10]; //The category of an actor, assigned by framework when create this actor.
    int (*Run)(Actor *actor);
    int (*Init)(Actor *actor);
    int (*Destroy)(Actor *actor);
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
    void *commBuffer;
} ActorFrameworkHandle;


typedef struct _ActorNew
{
    NodeMappingTable info;
    unsigned char data[16];
} ActorNew;


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


void CreateFrameMsgType();

void CreateActorMsgType();



int BroadcastCntlMessage(FrameMsgType msgType, int actor, void *pData, int dataLen);


int SendActorMessage(Actor *this, int destID, int msgType, void *pData, int dataLen);



int SendCntlMessage(FrameMsgType msgType, int node, int actor, void *pData, int dataLen);

int ReadActorMessage(Actor *this, ActorMessage *outMsg);

int FinalizeActorFramwork();

int ConfigActorFrame(FrameDesc desc);
int AddActorProfile(ActorDesc desc);

void DeleteHandle();

// Build the mapTable and create actor queue, if faied, return 0 , or return the count of the active nodes.
int BuildMapTable();

int InitActorFramework(int argc, char *argv[]);



int GetAvailiableNode(ActorDesc *pDesc);

int CreateActor(ActorNew *pnewActor);

int ProcessFrameworkMessage(int rank, FrameMessage *pMsg);

Actor *SearchActor(int actorID);

int DispatchActorMessage(int rank, ActorMessage *pMsg);

int FrameworkPoll();
int ActorRun();
int RunFramework();