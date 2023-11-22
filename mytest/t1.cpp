#include <iostream>
#include <queue>
#include <vector>

// 自定义比较函数对象
struct CustomCompare {
    std::vector<int> param; // 添加一个成员变量
    CustomCompare(std::vector<int> p) : param(p) {} // 构造函数，初始化成员变量

    bool operator()(const int& a, const int& b) const {
        return (a > b) && (a > param[0]); // 使用成员变量作为返回值的控制条件
    }
};

int main() {
    std::priority_queue<int, std::vector<int>, CustomCompare> pq(std::vector<int>{1,1}); // 创建一个优先队列，传入参数3

    pq.push(1);
    pq.push(2);
    pq.push(3);
    pq.push(4);

    while (!pq.empty()) {
        std::cout << pq.top() << " ";
        pq.pop();
    }

    return 0;
}