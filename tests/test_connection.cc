#include <iostream>
#include <math.h>
#include "ps/ps.h"
using namespace std;
using namespace ps;
#define ASYNC 1
#define SYNC 2
#define MAX_DIFF 2
int method = 5;

#define DEBUG

double gamma_mon = 0.9;

template <class Val>
class KVServerDefaultHandle1 {      //functor，用与处理server收到的来自worker的请求
public:
    // req_meta是存储该请求的一些元信息，即请求来自于哪个节点，发送给哪个节点等等
    // req_data即发送过来的数据
    // server即指向当前server对象的指针
    void operator() (const KVMeta& req_meta, const KVPairs<Val>& req_data, KVServer<Val>* server) {
        size_t n = req_data.keys.size();
        int work_id = (req_meta.sender - 9)/2;
#ifdef DEBUG
        std::cout << "worker id is " << work_id << "where it is push" << req_meta.push << std::endl;
#endif
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
                    for(int work_init_itr = 0; work_init_itr < NumWorkers(); ++work_init_itr){
                        grad[work_init_itr] = vector<float>(len, 0);
                    }
                }

                if(momentum.size() == 0){//第一次push，开辟空间
                    momentum = vector<float>(len, 0);
                }

                if (method != 5) {
                    ticks[work_id]++;
                }
                
                
                for(int work_itr = 0; work_itr < NumWorkers(); ++work_itr) {
                    cur_idx = 0;
                    for(int idx = 0; idx < len; ++idx){
                        grad[work_itr][idx] += req_data.vals[cur_idx++];
                    }
                }
                
                server->Response(req_meta, res);
            }
            else{ // pull
                if (method == 1) {
                    KVPairs<Val> res;
                    res.keys = req_data.keys;
                    res.lens.resize(res.keys.size());
    
                    res.lens[i] = grad[work_id].size();
                    for(int idx = 0; idx < res.lens[i]; ++idx){
                        res.vals.push_back(grad[work_id][idx]);
                        grad[work_id][idx] = 0;
                    }
    
                    server->Response(req_meta, res);
                } else if (method == 2){
                    if(ticks[work_id] > meta_queue.size()) {
                        meta_queue.push_back(std::vector<KVMeta>());
                        data_queue.push_back(std::vector<KVPairs<Val> >());
                    }
    
                    meta_queue[ticks[work_id] - last_tick].push_back(req_meta);
    
                    data_queue[ticks[work_id] - last_tick].push_back(req_data);
    
                    if(meta_queue[ticks[work_id] - last_tick].size() == NumWorkers()) {
                        for(int meta_queue_tick_itr = 0; meta_queue_tick_itr < meta_queue[ticks[work_id] - last_tick].size(); ++meta_queue_tick_itr) {
    
                            KVPairs<Val> res;
                            res.keys = data_queue[ticks[work_id] - last_tick][meta_queue_tick_itr].keys;
                            res.lens.resize(data_queue[ticks[work_id] - last_tick][meta_queue_tick_itr].keys.size());
    
                            res.lens[i] = grad[work_id].size();
                            //std::cout << "the i is hahaha" << i << std::endl;
                            for(int idx = 0; idx < res.lens[i]; ++idx){
    
                                int recv_id = (meta_queue[ticks[work_id] - last_tick][meta_queue_tick_itr].sender - 9)/2;
                                res.vals.push_back(grad[recv_id][idx]);
                                grad[recv_id][idx] = 0;
                                
                            }
                            server->Response(meta_queue[ticks[work_id] - last_tick][meta_queue_tick_itr], res);
                        }
                    }
                }else if (method == 3) {

                    for(int vec_itr = 0; vec_itr < meta_vec.size(); ++vec_itr) {

                        bool should_be_send = true;
                        int work_itr_tick = ticks[(meta_vec[vec_itr].sender - 9)/2];
                       
                        for(int tick_itr = 0; tick_itr < NumWorkers(); ++tick_itr) {
                            if(work_itr_tick - ticks[tick_itr] > MAX_DIFF) {
                                should_be_send = false;
                                break;
                            }
                        }

                        if(should_be_send) {
                            KVPairs<Val> res;
                            res.keys = data_vec[vec_itr].keys;
                            res.lens.resize(data_vec[vec_itr].keys.size());
            
                            res.lens[i] = grad[work_id].size();

                            int recv_id = (meta_vec[vec_itr].sender - 9)/2;
                            for(int idx = 0; idx < res.lens[i]; ++idx){
                                
                                
                                res.vals.push_back(grad[recv_id][idx]);
                                grad[recv_id][idx] = 0;
                                                            
                             }
                            
                            std::cout << "Now we send to worker" << recv_id << std::endl;
                            std::cout << "s the ticks is ";
                            for(auto v:ticks) {
                                cout << v << "  ";
                            }
                            std::cout << std::endl;
                            server->Response(meta_vec[vec_itr], res);

                            std::cout << "before erase";
                            for(auto v:meta_vec) {
                                cout << v.sender << " ";
                            }
                            std::cout << std::endl;
                            meta_vec.erase(meta_vec.begin() + vec_itr);
                            data_vec.erase(data_vec.begin() + vec_itr);
                            std::cout << "after erase";
                            for(auto v:meta_vec) {
                                cout << v.sender << " ";
                            }
                            std::cout << std::endl;
                            
                        }
                    }







                    int my_tick = ticks[work_id];

                    for(int work_itr = 0; work_itr < NumWorkers(); work_itr++) {
                        if((my_tick - ticks[work_itr]) > MAX_DIFF) {
                            meta_vec.push_back(req_meta);
                            data_vec.push_back(req_data);
                            std::cout << "we need to wait as the ticks is ";
                            for(auto v:ticks) {
                                cout << v << "  ";
                            }
                            std::cout << std::endl;
                            return;
                        }
                    }
                    
                    KVPairs<Val> res;
                    res.keys = req_data.keys;
                    res.lens.resize(res.keys.size());
    
                    res.lens[i] = grad[work_id].size();
                    for(int idx = 0; idx < res.lens[i]; ++idx){
                        res.vals.push_back(grad[work_id][idx]);
                        grad[work_id][idx] = 0;
                    }
    
                    server->Response(req_meta, res);


                } else if (method == 5){

                    // push_in()

                    if (ticks[work_id] < current_tick - 1) {

                        double exp = 1.0 * (current_tick - ticks[work_id]);
                        double momentum_cons = pow(gamma_mon, exp);

                        KVPairs<Val> res;
                        res.keys = req_data.keys;
                        res.lens.resize(res.keys.size());
        
                        res.lens[i] = grad[work_id].size();
                        for(int idx = 0; idx < res.lens[i]; ++idx){
                            res.vals.push_back(grad[current_tick - 1][idx] - grad[ticks[work_id]][idx]);

                            momentum[idx] += grad[work_id][idx] * momentum_cons;

                            grad[work_id][idx] = 0;
                        }

                        ticks[work_id] = current_tick - 1;

                        server->Response(req_meta, res);

                        return;

                    } else if(ticks[work_id] == current_tick - 1) {


                        meta_vec.push_back(req_meta);
                        data_vec.push_back(req_data);

                        double var = 0;
                        double thred = 0;
                        double ADB = 0;

                        int count = 0;

                        for (int j = 0; j < NumWorkers(); ++j) {

                            if(ticks[j] == current_tick - 1) {
                                count++;
                            }
                        }


                        for(int idx = 0; idx < grad[work_id].size(); ++idx){

                            double x2 = 0;
                            double x = 0;


                            for (int j = 0; j < NumWorkers(); ++j) {

                                if(ticks[j] == current_tick - 1) {
                                    x2 += (grad[j][idx] * grad[j][idx]);
                                    x += grad[j][idx];
                                }
                            }

                            ADB += (sqrt((x2 / count) - ((x / count) * (x / count))) / 8 + x / 64 * 2.0639) * (sqrt((x2 / count) - ((x / count) * (x / count))) / 8 + x / 64 * 2.0639);

                            var += (x2 / count) - ((x / count) * (x / count));
                            thred += ((x / count) * (x / count));
                        }

#ifdef DEBUG
                        std::cout << "For tick " << current_tick << "  " << (var / (64*count)) << " " << thred / count << "  " << ADB << std::endl;
#endif
                        if ((var / (64*count)) > thred / count && ADB > var/64) {

                            if (count == NumWorkers()) {
                                std::cout << "Force to sync " << current_tick << std::endl;
                            } else {
                                return;
                            }

                        }

                        KVPairs<Val> res;
                        res.keys = data_vec[0].keys;
                        res.lens.resize(data_vec[0].keys.size());
                        res.lens[i] = grad[work_id].size();


                        for(int idx = 0; idx < res.lens[i]; ++idx){
                            res.vals.push_back(momentum[idx]);
                            momentum[idx] = 0;
                        }

                        for (int j = 0; j < NumWorkers(); ++j) {
                            if(ticks[j] == current_tick - 1) {
                                for(int idx = 0; idx < res.lens[i]; ++idx){
                                    res.vals[idx] += grad[work_id][idx];
                                    grad[work_id][idx] = 0;
                                }
                            }

                            ticks[j] = current_tick;
                        }
                        
                        current_tick++;

                        for(int vec_itr = 0; vec_itr < meta_vec.size(); ++vec_itr) {
                            server->Response(meta_vec[vec_itr], res);
                        }

                        meta_vec.clear();

                    } else {


                        std::cout << ticks[work_id] <<"   " << current_tick - 1 << std::endl;
                        std::cout << "DEBUG, should not print this one" << std::endl;
                    }

                }
            }
        }
        
    }
private:
    std::vector<std::vector<float> > grad = std::vector<std::vector<float> >(NumWorkers());
    std::vector<int> ticks =  std::vector<int>(NumWorkers(), 0);
    std::vector<std::vector<KVMeta> > meta_queue;
    std::vector<std::vector<KVPairs<Val> > > data_queue;
    std::vector<std::vector<float> > grad_history;
    std::vector<float> momentum;


    std::vector<KVMeta> meta_vec;
    std::vector<KVPairs<Val> > data_vec;
    int last_tick = 1;
    int current_tick = 2;
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
    return 3;
}