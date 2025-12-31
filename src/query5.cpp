#include "query5.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <thread>
#include <mutex>
#include <algorithm>
#include <bits/stdc++.h>
using namespace std;
// Function to parse command line arguments
bool parseArgs(int argc, char* argv[], std::string& r_name, std::string& start_date, std::string& end_date, int& num_threads, std::string& table_path, std::string& result_path) {
    // TODO: Implement command line argument parsing
    // Example: --r_name ASIA --start_date 1994-01-01 --end_date 1995-01-01 --threads 4 --table_path /path/to/tables --result_path /path/to/results
    for (int i = 1; i < argc; i++) {
	    //cout<<"Args: "<<argv[i]<<endl;
	    string arg = argv[i];
	    if (arg == "--r_name" && i + 1 < argc)
		    r_name = argv[++i];
	    else if (arg == "--start_date" && i + 1 < argc)
		    start_date = argv[++i];
	    else if (arg == "--end_date" && i + 1 < argc)
		    end_date = argv[++i];
	    else if (arg == "--threads" && i + 1 < argc)
		    num_threads = stoi(argv[++i]);
	    else if (arg == "--table_path" && i + 1 < argc)
		    table_path = argv[++i];
	    else if (arg == "--result_path" && i + 1 < argc)
		    result_path = argv[++i];
	    else {
		    cout << "Unknown or incomplete argument: " << arg << "\n";
		    return false;
	    }
    }
    //cout<<r_name<<start_date<<end_date<<table_path<<result_path<<num_threads<<endl;
    return !(r_name.empty() || start_date.empty() || end_date.empty() || table_path.empty() || result_path.empty() || num_threads <= 0);

    //return false;
}
//split the db data 

static vector<string> split(const string& s, char delim) {
	vector<string> tokens;
	stringstream ss(s);
	string item;
	while (getline(ss, item, delim))
		tokens.push_back(item);
	return tokens;
}


//helper function: load_helper
static bool loadhelper(
    const string& file,
    const vector<string>& cols,
    vector<map<string,string>>& out)
{
	ifstream in(file);
	if (!in.is_open()) {
		cout << "Failed to open " << file << "\n";
		return false;
	}
	
	string line;
	while (getline(in, line)) {
		auto tokens = split(line, '|');
		map<string, string> row;
		
		for (size_t i = 0; i < cols.size(); i++)
			row[cols[i]] = tokens[i];
		out.push_back(move(row));
	}
	return true;
}



// Function to read TPCH data from the specified paths
bool readTPCHData(const std::string& table_path, std::vector<std::map<std::string, std::string>>& customer_data, std::vector<std::map<std::string, std::string>>& orders_data, std::vector<std::map<std::string, std::string>>& lineitem_data, std::vector<std::map<std::string, std::string>>& supplier_data, std::vector<std::map<std::string, std::string>>& nation_data, std::vector<std::map<std::string, std::string>>& region_data) {
    // TODO: Implement reading TPCH data from files
    return
	    loadhelper(table_path + "/customer.tbl",{"c_custkey", "c_name", "c_address", "c_nationkey"},customer_data) &&
	    loadhelper(table_path + "/orders.tbl",{"o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate"},orders_data) &
	    loadhelper(table_path + "/lineitem.tbl",{"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity", "l_extendedprice", "l_discount"},lineitem_data) && 
	    loadhelper(table_path + "/supplier.tbl", {"s_suppkey", "s_name", "s_address", "s_nationkey"}, supplier_data) &&
	    loadhelper(table_path + "/nation.tbl", {"n_nationkey", "n_name", "n_regionkey"}, nation_data) &&
	    loadhelper(table_path + "/region.tbl", {"r_regionkey", "r_name"}, region_data);
}
//Worker-utility Function
static void worker(int start, int end, const vector<map<string,string>>& lineitem_data, const unordered_set<int>& valid_suppliers, const unordered_map<int,int>& order_to_nation, unordered_map<int,double>& local_result)
{
	for (int i = start; i < end; i++) {
		const auto& l = lineitem_data[i];
		
		int suppkey = stoi(l.at("l_suppkey"));
		if (!valid_suppliers.count(suppkey))
			continue;
		
		int orderkey = std::stoi(l.at("l_orderkey"));
		auto it = order_to_nation.find(orderkey);
		
		if (it == order_to_nation.end())
			continue;
		
		double price = stod(l.at("l_extendedprice"));
		double discount = stod(l.at("l_discount"));
		
		local_result[it->second] += price * (1.0 - discount);
	}
}



// Function to execute TPCH Query 5 using multithreading
bool executeQuery5(const std::string& r_name, const std::string& start_date, const std::string& end_date, int num_threads, const std::vector<std::map<std::string, std::string>>& customer_data, const std::vector<std::map<std::string, std::string>>& orders_data, const std::vector<std::map<std::string, std::string>>& lineitem_data, const std::vector<std::map<std::string, std::string>>& supplier_data, const std::vector<std::map<std::string, std::string>>& nation_data, const std::vector<std::map<std::string, std::string>>& region_data, std::map<std::string, double>& results) {
    // TODO: Implement TPCH Query 5 using multithreading
    int regionkey = -1;
    for (const auto& r : region_data)
	    if (r.at("r_name") == r_name)
		    regionkey = stoi(r.at("r_regionkey"));
    
    unordered_map<int,string> nation_name;
    unordered_set<int> valid_nations;
    for (const auto& n : nation_data)
	    if (stoi(n.at("n_regionkey")) == regionkey) {
		    int nk = stoi(n.at("n_nationkey"));
		    valid_nations.insert(nk);
		    nation_name[nk] = n.at("n_name");
	    }
    
    unordered_set<int> valid_suppliers;
    for (const auto& s : supplier_data)
	    if (valid_nations.count(stoi(s.at("s_nationkey"))))
		    valid_suppliers.insert(stoi(s.at("s_suppkey")));
    
    unordered_map<int,int> customer_to_nation;
    for (const auto& c : customer_data)
	    if (valid_nations.count(stoi(c.at("c_nationkey"))))
		    customer_to_nation[stoi(c.at("c_custkey"))] =stoi(c.at("c_nationkey"));
    
    unordered_map<int,int> order_to_nation;
    for (const auto& o : orders_data) {
	    if (o.at("o_orderdate") >= start_date && o.at("o_orderdate") < end_date) {
		    int custkey = stoi(o.at("o_custkey"));
		    if (customer_to_nation.count(custkey))
			    order_to_nation[stoi(o.at("o_orderkey"))] = customer_to_nation[custkey];
	    }
    }
    
    vector<thread> threads;
    vector<unordered_map<int,double>> partials(num_threads);
    
    int chunk = lineitem_data.size() / num_threads;
    
    for (int i = 0; i < num_threads; i++) {
	    
	    int start = i * chunk;
	    int end = (i == num_threads - 1) ? lineitem_data.size(): start + chunk;
	    threads.emplace_back(worker, start, end, cref(lineitem_data), cref(valid_suppliers), cref(order_to_nation), ref(partials[i]));
    }
    
    for (auto& t : threads)
	    t.join();
    
    for (auto& mp : partials)
	    for (auto& [nk, rev] : mp)
		    results[nation_name[nk]] += rev;
    return true;
}

// Function to output results to the specified path
bool outputResults(const std::string& result_path, const std::map<std::string, double>& results) {
    // TODO: Implement outputting results to a file
    ofstream out(result_path);
    if (!out.is_open())
	    return false;
    for (const auto& [nation, revenue] : results)
	    out << nation << " | " << revenue << "\n";
    
    return true;
} 
