#include <algorithm>
#include <fstream>
#include <unordered_map>
#include <string>
#include <vector>
#include <iostream>
#include <thread>
#include <future>

using namespace std;

using uint    = uint32_t;
using int64   = int64_t;
using uint64  = uint64_t;

#define Val auto const&

#define CINFO(text)    { cout<<text<<"\n"; }

void print_help()
{
	CINFO("Usage: dedup PATH1 [PATH2] [-sN]");
	CINFO("Prints all files with duplicates within a list of blockhashes");
	CINFO("PATH1: path to output of blockhash. Build blockhash and prepare a list of blockhashes like so: \"find . -regextype posix-egrep -regex \".*\\.(png|jpe?g)$\" -type f -printf \'\"%p\"\\n\' | xargs -n1 -IF -P8 blockhash F >> hashes\"");
	CINFO("PATH2: if present duplicate wills be considered within PATH1 -> PATH2");
	CINFO("-sN: integer N, [1..100], cutoff similarity percentage");
}


int main(int c, char* args[])
{
	bool perc_arg = false;
	string path1, path2;
	int perc = 90;

	for(int i=1; i<c; ++i) {
		string s = args[i];

		if(perc_arg == true)
		{
			perc = std::clamp(std::stoi(s), 1, 100);
			perc_arg = false;
			continue;
		}

		if(!s.empty()  &&
				s[0] == '-')
		{
			if(s.size() > 2  &&
					s.substr(0, 2) == "-s")
			{
				perc = std::clamp(std::stoi(s.substr(2)), 1, 100);
				continue;
			}

			if(s.size() == 2  &&
					s.substr(0, 2) == "-s")
			{
				perc_arg = true;
				continue;
			}

			print_help();
			return 0;
		}

		if(path1.empty()) {
			path1 = s;
			continue;
		}

		if(path2.empty()) {
			path2 = s;
			continue;
		}

		print_help();
		return 0;
	}

	if(path1.empty())
	{
		print_help();
		return 0;
	}

	auto get_hashes = [](auto name, auto& alloc){
		ifstream file(name);
		unordered_map<uint, string> hashes;

		string s;
		uint i = 0;

		while (std::getline(file, s)) {
			for(uint j=0; j<4; ++j)	{
				uint64 h = std::stoul(s.substr(j * 16, 16), 0, 16);
				alloc.emplace_back(h);
			}

			if(!alloc[i * 4])
				alloc[i * 4] = 1;

			hashes.emplace(i * 4, s.substr(66));
			++i;
		}

		return hashes;
	};

	bool two_sets = !path2.empty();

	vector<uint64> alloc1, alloc2;
	unordered_map<uint, string> hashes1 = get_hashes(path1, alloc1)
			, hashes2 = two_sets ? get_hashes(path2, alloc2)
								 : unordered_map<uint, string>{ };

	Val total1 = hashes1.size()
			, total2 = hashes2.size();

	double treshhold = perc;

	//double maxdiff = 256.;
	int tresh = 256 * (1. - treshhold / 100.);

	auto &total_c = two_sets ? total2 : total1;
	auto &alloc_c = two_sets ? alloc2 : alloc1;
	auto &hashes_c = two_sets ? hashes2 : hashes1;

	mutex mut_;

	for(uint i1=0; i1<total1; ++i1) {
		//vector<string> names;
		bool named = false;

		uint idx1 = i1 * 4;
		if(alloc1[idx1]) {
			auto compare = [&](uint start, uint end){
				for(uint i2=start; i2<end; ++i2) {
					uint idx2 = i2 * 4;
					if(alloc_c[idx2]
							&& (two_sets || idx1 != idx2)) {
						int diff = 0;
						for(uint j=0; (j < 4) && (diff < tresh); ++j) {
							uint64 c1 = alloc1[idx1 + j];
							uint64 c2 = alloc_c[idx2 + j];

							uint64 d = c1 ^ c2;
							int bits = 0;
							while(d > 0) {
								bits += d & 1;
								d >>= 1;
							}

							diff += bits;
						}

						if (diff < tresh)
						{
							//names.emplace_back(hashes[idx2]);
							lock_guard l(mut_);
							CINFO(hashes_c[idx2]);

							named = true;
							//double similarity = 100. * (1. - double(diff) / maxdiff);

							alloc_c[idx2] = 0;
							//CINFO(hashes[idx1]<<"\n"<<hashes[idx2]<<"\nsimilarity "<<similarity<<"%");
						}
					}
				}
			};

			int cores_num = int(thread::hardware_concurrency())
					, start = int(two_sets ? 0 : i1)
					, step = std::max(1, (int(total_c) - start) / cores_num);

			vector<future<void>> handles;

			for(int i=start; i<int(total_c); i+=step) {
				int st = i
						, ed = std::min(int(total_c), i + step);

				handles.emplace_back(async(launch::async, compare, st, ed));
			}

			for(auto &h: handles)
				h.wait();
		}

		if(named) {
			CINFO(hashes1[idx1]<<"\n");
		}

		if(i1 % 100)
		{
			std::cerr<<"processed "<<(double(i1) * 100. / total1)<<"% files"<<endl;
		}
	}

	return 0;
}
