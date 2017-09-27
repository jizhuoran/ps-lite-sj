#include <iostream>
#include "ps/ps.h"
using namespace std;
using namespace ps;


template <class Val>
class KVServerDefaultHandle1 {      //functor，用与处理server收到的来自worker的请求
public:
    // req_meta是存储该请求的一些元信息，即请求来自于哪个节点，发送给哪个节点等等
    // req_data即发送过来的数据
    // server即指向当前server对象的指针
    void operator() (const KVMeta& req_meta, const KVPairs<Val>& req_data, KVServer<Val>* server) {
        size_t n = req_data.keys.size();
        int work_id = req_meta.sender - 9;


        
        if (req_meta.push) { // push
            
        } else {            // pull
            
        }

        size_t cur_idx = 0;
        for (size_t i = 0;i < n; ++i) {

            Key key = req_data.keys[i];
            if(req_meta.push){ //push
                
                KVPairs<Val> res;
                CHECK_EQ(n, req_data.lens.size());
                int len = req_data.lens[i];
                if(grad[work_id].size() == 0){//第一次push，开辟空间
                    grad[work_id] = vector<float>(len, 0);
                }

                ticks[work_id]++;

                for(int idx = 0; idx < len; ++idx){

                    for(int work_itr = 0; work_itr < NumWorkers(); ++work_itr) {
                        grad[work_itr][idx] += req_data.vals[cur_idx++];
                    }
                    
#ifdef DEBUG
                    std::cout << grad[work_id][idx] << " ";
#endif
                }

                server->Response(req_meta, res);
            }
            else{ // pull
                
                
                if(ticks[work_id] > queue.size()) {
                    queue.push_back(std::vector<KVMeta>());
                }

                queue[ticks[work_id] - last_tick].push_back(req_meta);

                if(queue[ticks[work_id] - last_tick].size() == NumWorkers()) {
                    for(int queue_tick_itr = 0; queue_tick_itr < queue[ticks[work_id] - last_tick].size(); ++queue_tick_itr) {

                        KVPairs<Val> res;
                        res.keys = req_data.keys;
                        res.lens.resize(res.keys.size());

                        res.lens[i] = grad[work_id].size();
                        for(int idx = 0; idx < res.lens[i]; ++idx){
                            res.vals.push_back(grad[work_id][idx]);
                            grad[work_id][idx] = 0;
                        }

                        server->Response(queue[ticks[work_id] - last_tick][queue_tick_itr], res);
                    }
                }

                
            }
        }
        
    }
private:
    std::vector<std::vector<float> > grad = std::vector<std::vector<float> >(NumWorkers());
    std::vector<int> ticks =  std::vector<int>(NumWorkers(), 0);
    std::vector<std::vector<KVMeta> > queue;
    int last_tick = 1;
};

void StartServer() {
    if (!IsServer()) return;
    cout << "num of workers[" << NumWorkers() << "]" << endl;
    cout << "num of servers[" << NumServers() << "]" << endl;
    auto server = new KVServer<float>(0);
    server->set_request_handle(KVServerDefaultHandle1<float>());   //注册functor
    RegisterExitCallback([server](){ delete server; });
}

int main(int argc, char* argv[]) {
    StartServer();
    Start();    //启动,Postoffice::start()
    Finalize(); //结束。每个节点都需要执行这个函数。
    return 0;
}