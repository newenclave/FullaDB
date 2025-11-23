#include <filesystem>
#include <vector>
#include <map>

#include "tests.hpp"

#include "fulla/bpt/paged/model.hpp"
#include "fulla/storage/file_device.hpp"
#include "fulla/page/header.hpp"
#include "fulla/page/bpt_inode.hpp"
#include "fulla/page/bpt_leaf.hpp"
#include "fulla/bpt/tree.hpp"

#include "fulla/codec/prop.hpp"

namespace {
	using fulla::core::byte_buffer;
	using fulla::core::byte_view;
	using fulla::core::byte_span;
	using fulla::core::byte;

	using namespace fulla::storage;
	using namespace fulla::bpt;
	using namespace fulla::codec;

	using file_device = fulla::storage::file_device;

	using model_type = paged::model<file_device>;
	using bpt_type = fulla::bpt::tree<model_type>;

	using key_like_type = typename model_type::key_like_type;
	using key_out_type = typename model_type::key_out_type;
	using value_in_type = typename model_type::value_in_type;
	using value_out_type = typename model_type::value_out_type;

	using page_header_type = fulla::page::page_header;
	using page_view_type = typename model_type::page_view_type;

	using file_device = fulla::storage::file_device;

	static std::filesystem::path temp_file(const char* stem) {
		namespace fs = std::filesystem;
		static std::random_device rd;
		auto p = fs::temp_directory_path() / (std::string(stem) + "_" + std::to_string(rd()) + ".bin");
		return p;
	}


	static std::string get_random_string(std::size_t min_len, std::size_t max_len = 20) {
		static std::random_device rd;
		static std::mt19937 gen(rd());

		const std::string chars =
			"0123456789"
			"ABCDEFGHIJKLMNOPQRSTUVWXYZ"
			"abcdefghijklmnopqrstuvwxyz";

		std::uniform_int_distribution<std::size_t> len_dist(min_len, max_len);
		std::uniform_int_distribution<std::size_t> char_dist(0, chars.size() - 1);

		std::size_t len = len_dist(gen);
		std::string res;
		res.reserve(len);

		for (std::size_t i = 0; i < len; ++i) {
			res.push_back(chars[char_dist(gen)]);
		}

		return res;
	}


	constexpr static const auto DEFAULT_BUFFER_SIZE = 4096UL;

	key_like_type as_key_like(const std::string& val) {
		return { .key = byte_view{ reinterpret_cast<const byte*>(val.data()), val.size()} };
	}

	value_in_type as_value_in(const std::string& val) {
		return { .val = byte_view{ reinterpret_cast<const byte*>(val.data()), val.size()} };
	}
	std::string as_string(value_out_type val) {
		return { (const char*)val.val.data(),val.val.size() };
	}

	void validate_keys(bpt_type& t) {
		std::optional<key_out_type> last;
		auto less_type = fulla::page::make_record_less();
		std::size_t count = 0;
		//std::cout << "\n\n";
		for (auto& k : t) {
			++count;
			if (last.has_value()) {
				CHECK(less_type(last->key, k.first.key));
			}
			//std::cout << "\"" << as_string(k.second) << "\", ";

		}
		//std::cout << "\n\n";
		//t.dump();
		//std::cout << "\n\n";
		(void)count;
	}
	template <typename C1, typename C2>
	bool compare(const C1& c1, const C2& c2) {
		return std::is_eq(std::lexicographical_compare_three_way(
			c1.begin(), c1.end(),
			c2.begin(), c2.end()
		));
	}
}

TEST_SUITE("bpt/paged/model bpt") {

	TEST_CASE("creating") {

		auto path = temp_file("test_page_model");
		{
			constexpr static const auto small_buffer_size = DEFAULT_BUFFER_SIZE;
			constexpr static const auto element_mximum = 10000;

			file_device dev(path, small_buffer_size);
			using BM = buffer_manager<file_device>;
			BM bm(dev, 40);
			static std::random_device rd;
			static std::mt19937 gen(rd());

			SUBCASE("create tree") {
				bpt_type bpt(bm);
				std::map<std::string, std::string> test;

				std::vector<std::string> tests_values = {
					"D0vvntWD9264ehOJTGwHAvqTUIB3sv1C3WkSnK5", "uWQFtsYvBoCxvteOvmJojhJVHUfp0TokvF1NGnYNnEzXx3kkyvjZswi4uVubMVjpfbEJQcFkbA", "R2CN0V2XwpZzr", "iixprOHFIfFz03uQAwgXQhTxQLcXXOihV51zvPSeQOlfEp0NC7BoqJZZOQYTScEOs7VrduOBFtKie", "EIZXfvCwkAqZsOv7UkUulGcRhrkTYpoMXhXRDCmHap3A", "OvDw9MVLU57shYR78M", "BiDg1yfm3l45qVsbD25k5Km1b56ALKHCB2kn2OuT1KZn8e1WrXSLK", "5LXRpHF9WPPH17mCfkpLejbr", "BkHDVo7UQuVKyHR2NITSm8Tc8", "0T6UhhjXK5", "9UtPgCrt7FfbukP8qlE4rZFyx7wANn82gE7J2BtzZXzmJBTsplDR8W1E6y1MbSvHOWXaEL003Hbip6ZnqSFMX", "2b3CS0", "svkq2zXIZ4CjggnkgSAypmIZp5NjwarS8qGNSe4m0YKKpilKjnrQ9T", "NqNNv5z4OEMBbotkQGWtd5Zs0rMp5sq3v0Y8tspppOL8Fskvcrf", "Bw2KmsqntS5Nxal9w17bkTsVhzmky9jpRlHfCLFQJgqjP3qhVQKyK9", "LiuWmOacLKetMDVy46uyCSF2Dpe4PT", "0FcnC3T0lGhFfkTYMCMxQ63IhbGsSUyKcsHsD1fKKehDoT", "DMr5Q9MW2emzJhQ5qG66D3ue6x00AVbPZulsH", "mQLK5Zw2f7ltWmQd4Sjs9L49oyWpjUb444uAQaMDkiC1sKiSEsbNaJcwXX2Jjrz", "pNTQaQMftcDwqEcWW25fLJLwZCftZGtOa9d47fufqaGzz8Lgb5MvUEkbW", "wgvI8pBhxKHmgF30gOZ3ZwHC1a12CyPM0K51lSRzvRG1t", "T5ps7AJy3Kk0wc7rWUOduEGggx1k9XKy85yaJFTRuUl8OdCSbDhzQj3uJHDCT0", "4IWjjBEpPmWsg2urmvN0NsBBxWofGOX", "XsHUb9rBFYuZyUFmwIHZjJT9m4fCQbJWXgE2hCVvKY3RcMDQ", "dtjtGLNH5aPITs9uj0ihPKrETptXkLVq", "m8pak2J5sWrEbc0kh9eCh11mwEiYYh8xsKQsSgT3MZLwjNprI5GB1TTh611DhNfC9lWzOpGaMK4vcCB2", "jNJHPw1lCWI5960SoGt9484", "YRX0Mj1RlvXbJdxAxCo25brNw4wQojqHTkGNVUAGRlo0flClzc2Yaz8jvwJL579C7X7DIM", "3ipsEquJLUPnbUIj76vInNr5DUZfXurO3", "lhVXQjdxAI3W8K202IncOTi4RrbGNqOsniuHtpNIhgVKM0ZNoC8QTiJ0KHTfBdr1vrqM6g3dPRSri3UakfXoHWrr", "72UpeR077wkCbFL1j03yGey9OzWT1i3y3F1rgIQqCZzAFvuV51NNLmA5iEihfNKHmPBHfKMj7iJpRPz3n4Q", "kg1SsWzg3VGPMZ3tKLmhmZUEAJ77vv7k5isEoeYq1ZRTGvAVGbCjh5QgSz6TNSLfGJJgivI", "WMy7UtlXPlc1fG76I9MVE0JNUCoaJJ6Kdt5koxNSBole8KtWBkglPh8yudz4gYh5vevGaKx2YuDMjIOb58rN0fure", "HWgr7vSrhKi9s6d8EdABJIWZp9dwVwP5tBuf8ZUC5jRzesPqvAGRTFE7JpJawUdD", "qAFc5OJF", "AL2xthG", "9dy8i", "h1q7pggxuHBGJsRFgPo7lZY2FMt5lVhTKjZ24VVBdNul31J83", "bK5UbqHcbBQgtadKrRinuXhSswmcjtqJgEd", "4n953msasnBbgfgYAe7iEfIm5mVhM5nEcvE5fxsxx8Wq3GOxi", "3S3vPpcJ", "p4lA2xri54WPDWVJ", "zW9Q9UbNeTwjwVmKKFQpDxGzsXASguyntYJ6HT7wvBsakY9qk0zqgPhr05", "KR3NhbS0S04IiesBeFvgm7PLIGQ8fOQAbf2UH68IlHTJS9Vxj6rRVZSAG0zVB5vjY5aLr5DjRkO9U1cC3gJKEd", "bDxNUloT", "IdVBIwh6vp5g7vIlHPz2zD6cp0bxsgGPvRNE8igBMvYRDhstAqF4FFbKMwsqUluzE", "bJ4xSBSVQxIKglU9pv7jlS", "FM1L3X5opB7YT5mEWxbHIb2JPdKzOEuosYNCZmgv2wvV", "5m77z5NjC8Y5VWvZYU7jXNHVltiBOHq33RqXk9PDLK1769rfNbLZ9Zn9iw9WPbXFEhC5iaJqzmE3gcp6jtyPcREEKs", "kzczAsSpU2BkBeYKEV5NSH2K2atlt9oPvFhKwuBg3xXbPvKPLsHKEhOPM1i5DDyOkFJpiJc", "EuZMu", "rjpJpgfg2EcNYDffRsTQzBRJXp9dMf3nTU0q", "gaLBlBDUu24qaSF0KiAhh2", "7W8ccsosmAbUb7GG694NEmeryf9y", "SsDsL5OiwngRDr", "hahSCkBROJQ7zg1o4", "n7jGib", "5IH87d8lmPi", "EUfbd7EfEbEz4jDJBXOUMZMvWkKdDOdpeEaflXAGiGhgIbyxdcPVWlxVbzRXGSzBl5vXq0GbAnrLM07Fn9MC", "YwRoj8lpf5JkJlpikoPCuS7l8ynDNz2WoicMPA9zsljFJYnlcQ", "I5sI48BHlkk9sX58uC7LUl9UEQi85yzyOHjoJU6U5C7uhWJWlsqXsmDIRfQCeApGnVv2s441DfJ2Fm1Xt9", "Sm6hI2aiYjyJzK1OLD7yWXXGV1kSYdtRVJHGa4NbiHXhtLw0bzG6D04oavpcvUsQkz5JPGDw11K", "UyqT7bjnChWUEqMEMX6E7G2GomTiqOgvi3OLfpq2M0MmyxdZIi34YKdgimVDnHJoSx0vll", "cPbEP", "SSFuLndkWApPlXhRPfmlfiDr5", "tfr7DLVDOFvuHYwV4HDyp", "xMB6QiZVizS7WAuJn5FEWjronS918XsG0lBiox15IcBoEx49Go8", "otGDYkqBESg5Z5fwyt4WjAZTnokEC8gXL2nnPo9qHfIL7OUi0DOZayOkW7B7ZjKzu", "WpAtJ7U", "CuuXhASQGRrwDgwi5EpFT6SJWYzfDrv4KG949Z9HYyW59gLoF", "tUkkbY9WF4wEEwME3u4F9owjfJaRr9ePPhyxKlJKWTr7Ljcf0cA1Xupi7PqmlDsW", "hsZrxyG1OZviVrQXJBZdRYuydpYO8nD2YOSJwU41n3zKfdWtb9CuiNo6aPpu0IrwvrBdvehvqVA7y", "lAwbtiiAGFFvSoYWDQAVacDQgAmG8apbyzpVPccKNvdFmLAosTLF9qaHbSDhwp9", "VHHa614WHnomZgyNiCBcuj5ldV", "7YSsNW4WUIxq73ilDxqgSoADwjdCembvSPZm1VaEmdOGz8xCD", "noitZk9mbIkYB1zI6VLrqpE62o8pTYG6kvaH40g", "4TKJE3FvGbShWJI", "hbVOHSFhUnfJBazgzxCyzjRjnRHAqggE7157018hmKOydxKKC368Q0BKArXF1qyI3p", "PaSrav", "qxVbXqY8Umt5HEzKxNmxBfhMvuwyY55G6rxYzHzjqJUcLR", "AUdyD6", "6FNVBQnRbtmTr8P0omYHXCeixQTwHLQBwAGGsbb2rius3XqXkNYOzkvBFi35cgmlD1wiWWui1px0mJ16t0ZABZf", "bHVhyRiZ2u9RMcCrmbal4zOMeCnirJesvhEIklRXAs", "DYlPPH", "uNRF5bWHchw", "HltkTi3Ee3Ui8Odd9VpqwKoedn5mlti0MaygfS1U", "9MqAtdSuSiiBoBZu", "DA1REzM4VJ0b2CBf364onfWRAA6ICDoi5QoYoqTVJGHCsSA2NUeJZVJBMbA2slCbqeXc1r", "HJdjM3s3WMAk4WQQYNuj0kutT4zR0Ng6bBYzu2mojmBqTRk4jlhTkjBGldO2zfWytbfBhsyJ0swS5QVuLgdeWP", "OYzcoAd2Ah1oph38L23iIDVJxqtIrngRhvCxCV3opf9XbE12xj3mKa32HSqYW8Zxt0PAq3fONeAyzlTsia", "SfBUyxTePZUCm9oZjV9vvQ6vWEzJYtPcZX", "oZBMT0mkiXE5tRH4WguyN1uNCb8kWNFr30xzCOhXnE2Ms7", "e9YFAX14KQSIfIEwhiEFa8775y7uu0kcBDgoanv26ZRSIJsm9G59Zj", "vwPz1RebzhcL6QpcALYtrKtiEeurlkbSpXsK2XF1r6IpYa0KQim8SWBcExotfbxxzlFzs1jMX5v", "MVI2ZNeYsjKa7IvQXYctlkJysKRa3mLGYsZMuQzTrCtJBrZacN0JsRYUJoLSpXoNLIqc2Oh76gM9tRcxTP", "aS4kj10QRAmlD2H9Hpv7mUnqN5zYeFDpdmal7rXjljQxqIxK9oBwZdfmQqmzp6BJ", "DRLMeMAUYa3HYulzqCIySBXacdIHcNBP2BzQ2NN5CVLnHL3M", "ZRqSRzlT1iruAHQW5cpT6GeF8axtjHf0", "yCqTGu2tCW1sL", "Eu8nBlIpPDyGtyxsgpYZGLHqwJDno" 
				};

				std::cout << "File: " << path.string() << "\n";

				for (unsigned int i = 0; i < element_mximum; ++i) {
					//auto ts = tests_values[i];
					auto ts = get_random_string(5, 90);

					auto key = prop::make_record(prop::str{ts});
					//std::cout << "\"" << ts << "\", ";
					if (i == 88) {
						std::cout << "";
					}
					if (!test.contains(ts)) {
						test[ts] = ts;
						CHECK(bm.has_free_frames());
						REQUIRE(bpt.insert(key_like_type{ key.view() }, as_value_in(ts), 
							policies::insert::insert, policies::rebalance::force_split));
						//bpt.dump();
					}

					else {
						std::cout << "";
					}
				}

				const auto check_map = [&]() {
					int id = 0;
					for (auto& ti : test) {
						auto key = prop::make_record(prop::str{ ti.first });
						auto it = bpt.find(key_like_type{ key.view() });
						CHECK(bpt.end() != it);
						auto val = ti.second;
						CHECK(compare(as_value_in(val).val, it->second.val));
						id++;
					}
				};

				validate_keys(bpt);
				check_map();

				//bpt.dump();
				//std::cout << "=====================\n";

				const auto tsize = test.size();
				for (unsigned int i = 0; i < tsize / 2; ++i) {

					std::uniform_int_distribution<std::size_t> dist(0, test.size() - 1);
					auto idx = dist(gen);
					auto itr = test.begin();
					std::advance(itr, idx);

					auto key = prop::make_record(prop::str{ itr->first });
					auto it = bpt.find(key_like_type{ key.view() });

					CHECK(bpt.end() != it);
					test.erase(itr);
					bpt.erase(it);
				}

				for (auto &t: test) {
					auto ts = get_random_string(5, 10);
					auto key = prop::make_record(prop::str{ t.first });
					REQUIRE(bpt.update(key_like_type{ key.view() }, as_value_in(ts), policies::rebalance::neighbor_share));
					test[t.first] = ts;
				}

				validate_keys(bpt);
				check_map();

				//bpt.dump();
			}
		}
		CHECK(std::filesystem::remove(path));
	}
}