#include <filesystem>
#include <vector>
#include <map>

#include "tests.hpp"

#include "fulla/bpt/paged/model.hpp"
#include "fulla/storage/file_block_device.hpp"
#include "fulla/storage/memory_block_device.hpp"

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

	using key_like_type = typename paged::model_common::key_like_type;
	using key_out_type = typename paged::model_common::key_out_type;
	using value_in_type = typename paged::model_common::value_in_type;
	using value_out_type = typename paged::model_common::value_out_type;

	using page_header_type = fulla::page::page_header;
	using page_view_type = typename paged::model_common::page_view_type;

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

	template <typename TreeT>
	void validate_keys(TreeT& t) {
		std::optional<key_out_type> last;
		auto less_type = fulla::page::make_record_less();
		std::size_t count = 0;
		for (auto& k : t) {
			++count;
			if (last.has_value()) {
				CHECK(less_type(last->key, k.first.key));
			}

		}
		(void)count;
	}
	template <typename C1, typename C2>
	bool compare(const C1& c1, const C2& c2) {
		return std::is_eq(std::lexicographical_compare_three_way(
			c1.begin(), c1.end(),
			c2.begin(), c2.end()
		));
	}

	struct string_less {
		bool operator ()(byte_view a, byte_view b) const noexcept {
			return std::is_lt(compare(a, b));
		}
		auto compare(byte_view a, byte_view b) const noexcept {
			return std::lexicographical_compare_three_way(
				a.begin(), a.end(),
				b.begin(), b.end()
			);
		}
	};

}

TEST_SUITE("bpt/paged/model bpt") {

	TEST_CASE("creating") {

		auto path = temp_file("test_page_model");
		{
			constexpr static const auto small_buffer_size = DEFAULT_BUFFER_SIZE;
			constexpr static const auto element_mximum = 10000;

			memory_block_device mem(small_buffer_size);
			using model_type = paged::model<memory_block_device>;
			using bpt_type = fulla::bpt::tree<model_type>;

			using BM = buffer_manager<memory_block_device>;
			BM bm(mem, 40);
			static std::random_device rd;
			static std::mt19937 gen(rd());

			SUBCASE("create tree") {
				bpt_type bpt(bm);
				bpt.set_rebalance_policy(policies::rebalance::neighbor_share);
				bpt.get_accessor().check_create_root_node();
				std::map<std::string, std::string> test;

				std::vector<std::string> tests_values = {
					"QkzS4grjiw0vk1GsNZn3PyCUGNJhdpgw8oI353KKQDfPHm1bsKN", "JgyzQtMXp5", "2L1WMKJ9PXnlaufFMCDWAmXe7XwhZgv2UKjTjwyv8RJIICn7fCyktlV8Tj7DYBMTGaIQCqR", "e5O4HjhOPMZs08TwIOn4L9KB0aBiLh6hE7HPAQFubYuGdl33Va", "FJXBO9Cv4CCvxa9m7rIVol75dCf", "qaiC3sO7VEn63pcBar6cYLFF8Gx71Yd", "1Cce10zGckqwAJ6eYtT1xcoduRArydXTEqe6eL3qnJfOtAYgV2x", "jGw9MO", "TtQuLVzybhmBQY1vWr97Ui", "Omofu76X4DX3acho2YYg8QpdwYCW71Jt10rcZShK9IU5JuG6faKw0mEO99ugLMTarysUIUqvvzG5wpwYs", "k4gaWIGGMoZsmAVYiwsdvUKHY9bCVZeWfKCCKIa75HLJSH7dwarairWQ0CTkTUoGK34", "HAyjig4xiYffCxnh8gnxoaV7SHXiJiAzZ70Y3L6jUGBGGKNa", "uSbbRwMxDzPaA16dMBGUz", "50xU7cQihKpidtmeLWuN07pMkvmkgXFZERzYaVL", "YKfbI4OBnhEHX6st374rKmjj4nR0nVCNzDdHeneFczNeRTRJUsYbs98wrU8ilDOA8wvFRUGCAzW2vo0", "VZceSQLMK8zg8vJEVDeZYjjJxDOrhKeFvnHrc7VUfpu3EtPjtEqoNjGigM3fKvpLa3hV", "arxjIav63Hhg56SJq76ruuHglDRd5", "1yZJJ9P9GP3WHz2mKKqg8c3J2rHGKn0Xpd2LdN55GvS6P8S2TbQs0OBAya5qKoviJYTs1rQpj", "EZLQqFMegtP8nHCrt0E7vEpNACkcQXGO9XIMzNHdPGmFNVQb", "G9VaLG5CIACLdX9832HWyxKXkA2TvbWMHJZtFToEaK0ioTIAeINOz5RmwtrF7apqDPcDk2XOhJDt", "TqGYkcVgj2EBZYL9", "q8E2XYQtSA0ZL16ziwTHBt4W3AsZpjQaKDFy9tImpbM8P1U52OrvHCaXDzjcUgyOeZrtIFtu", "Pgbyf5MLftqbvdsPsi9qqqLndI0Xkpin9t9Fb3VJHHRvl6YnUK4CgUT7zLv8xd5JpKsMT", "qrrly823", "7spnSLz4NWt8hwBBDzEGTJ6pb3wpQSslIjLdfGs5yrADWTPFMuIOdxE2aez", "cNSmJkycG5BJMUnMo8P8w33is7fSwYTkscVulklybbXADxpdxnYyq25Z5hdXKaRpGvtuHP", "HPjVz6A6t4aDdaTQriO6Mkk9e70zK", "MiiHsw8s0KvNLh5VO95voK5JlnPKLR9WbfYhaF0PSpUgu6aqCP5J", "Cs5gr8n9nbRqTGahbCyS40vFkg7cdKyk8rL6IshvoDCm5GSX9lHRdPLhq1J9zjE8VaViNPdDXiODUhHCDC8V", "39eKldOvEVAw4uSozlrhvIS479FXCCxpe4r5szfOOIP0kuYMChanq2o6TFbYGxySPB6BBLVSQEDyVX", "zkMn7RrzTlW8aguujqjCt", "5GswuaOxK803SI2Wrd2jKvGHUqsRF0aq7YS0hRSx4", "kQnJHyxglSVDJ5mYYvI08MHlbWb3KADqY6tumnek32WU4AX9hlklnqV8gWH1U", "H5nqufEBB6W0vREts94uoqQCb4YhpQOFDY9ux87LxlWXwThuArRH9PslkIHOiBnenWSjp9lH04mx8", "jnNvPZHUtphf1ID5PNC20AT7ib6wYt9hgTNTv5G8xKo9oiuQQS3G4P0lAyXA68wNl", "kTi1hV6X", "dpIzOnkbPnIiGzuZGZDp5PweUHrw", "rXHqPX8PRCbKN92MIc1SFGnIiCdOnoxH6KEh01Ie7I4Sw8UpCnXc1yjCVirwFalKKoCQdlPapHbnF7NGoY", "PfjW7NCVpAMXGKDKwK6AMVdcq9TG1asi", "PiWUnxYKB52SfWyPFxL02hZEZHeOc7It2h8Hh5etux0fxClEDOJdxlyvguoK4z4tyHIKMpI2jduzIqoY", "DZCMrOSi1P", "WdyKzBdYVgzB7F", "ZaFr2PzImx8buyVSVk2HW9dabnESRRhhuGd58YrNCYvCIbDgsHYdD8xDwScb", "7lxRcmi8cr398NidOncMEtsjKPFKrM8yPHZyqab4lCTJOTMYVVRxjVFHQuSUnIf", "VLQ9iUUNrpVthZ4vvhLBxGPTxnnuX0dJY7cWtRsHlvS9yDSvWGCc0lFIyjxYkLam71dbQb6vAxDG", "JF5kYX4iEq20GFoB7WUfwlFgCRGlHQ0KwcNk8jtBjwWsMIkiOGXWGRfAOUTiwtHJxreZVQCjAV5uIVUMNeP", "sZcdh4Jfp9Fi4pXLRtVox9GOjBMBRGws2isBVPypWjqf3XGV6Z2cKHYDHAMjHBPKYfg3A", "pTB2Y0oeiseVSWT6cUoJKVBVRnmmp3zTHkfHvL2chzaJ8ewjKg2K9d4BmooqHGOU", "qXpvRMeyRtdxc9hWMH2wSu3IBFGLb9GSGvBrQDODB2Eh7pESFAMckWTonle9dnixrcuOrKGTQiePBp1dO2zw", "VcJqRDITrZDjfESw4mDbRWX", "4R7CD3GDZ9Poy7hhggKqPzcwjC7G5HomAX6IVyBzqxMP4BFrZd0UPZ55aoluXwecFJnAfNjxGDDjuD", "71pF3aMFgT9a5", "h3KL3IBq48KkxBbHyoz1Xhc4wlcPamaZ2mU4KZTLl9krgFZf", "gaYc7mLrbOGeeSgldXtkv7skpZpbim8Cp2Bw", "9601V70KbbyQIw", "Mdi8FCwJTnUVYjxTKvOD7IjoxRnuxcKWMWm6IWxsUNw9aACsGcUUd3LrbJQY8yo", "eptKsA3JhvlzaYMU7rUtGqQbfJkrRWdezDtsZHkw0jFMcgl84", "V3KK2F6MY41SCDJVS9wNKLUAr8gczT0", "IuZ0jmRFFeOEW5V42BYJOPuJ2KQWRurcLibJcaEyXYyS", "pBgYM8vYPgOyrPM8zNiRKIZq", "6j5AqvQmZLqIkVU2", "Rv4iLlb4XWnKFQYUHf6H6gQB3YgfEj37A4RYXJZi19PNYp6YcmGv7DMWfnW3jOEmU", "KpgMHBF3qFgdcprK4Pgsx", "YpiFjhJ", "q0MkZTFvwYD2Tdn7fGGZyii2V5Dikyd0AykFTfrG1XvS9sbzpATytRetW1Ka5LW", "iJ68SL6k9L9YvLrRjCuJAasn0FdEAqxsTZQb32VTkWCl1zqnk", "ETB2UmdCZrfF2Jm4ZfnU1HgR3Ga1SMjmW0oxbXFw", "aJ4Lof3PCy3gllwbioC784OcJAKBklfWf35DAwTt9", "tdFxbi00TUYu0Og5s8uwWJVjbL6SSet5JJ5vIPlcgh", "fHno36pQMIbvRpBsNoibcI6w6F179edd78seln5L0UBdIMfcQNent9do8s8", "x9iq1v427mQc5YdPb0rP1KUkw5blpWM7Y11NI1uRKl8obrpd3PSe0g3RWWUydb2F15CWTmXrngRIg", "BO6voFYQ9PE40sgqvyJLTREaPx", "qJDx4ZWPVivATkE", "mPNo5XYluiSyGew4FrCTzd1uvCHSRHk9", "jBrzHuqSQwa2ou", "xSEkkblAzK71", "IuHTU8YoARW1gyzQP0E", "7MzvGn8o8JNV2yfWL1mZPrCB6swDcpKT6PfwV", "E2akDQmye8nWjioKgxC", "RfxP60AgNZs", "Vydra6chhjQdLxzw8Cg1g9nZtzXkwWVd36k5TflOY4e4MkrSTRIfM", "6ocwxPI6KkR3jeyfxFGdY3j2utCQqfYLEqcHjIUriCuttrbbAWClZgBXufAhOfo", "WCj9YftZR68IbwRar4DzlZLKbAjcksWG8hROqozNZzLmtWIbZzWlTUoLdeVQ7c1mWjoGGLFAD6vT", "wyb8vw", "knEmPQRe3bFfsLxFPyVLAoH6MD09ouQw7mkq3lJLwptLy6TxAv", "65KrSaKH4zfU5Zy0yZcWi1jNVeBNKoiFtbtCVI6MkK0", "gNQACeYr64MWK8CaL0ion5iZBUQpIUrep5hsq6GQ6DqZm6gr0XfAFjWRIJRnMm0", "LWgWIWHOVAqS6XLEXz6uBE4nH5LKr3nJsItXSWXhUY3XWYeJ4s5y9A3cFzT2MS1NCiCFM", "xtME0jo6Jy3wQdrtg", "WYUpgpXqcHEayNImQLZlDLYWhPTutitClCd7xruBNbP03Efk7Ki4jcI1P6zHsZ4EZ7tIL7zInTMG", "H88IXCnVOkwEaUZge8RbjNTMN9ARctwHr", "AFyASJXQPpVb2HfDKptY1WbOEgKnrZNOC6Ssr3D1R0sH7FktNBixmoVtGoS8D7MCCe2PGiQrVK1Q6iHHUgdmTDF", "HRT4QwkyK4GhIEgTqN6xZAx8QODYS8tISb45pS8t3ayzAf35GRxec0yYWLchYgmxAXZhk6BT0c70", "2eda31zqC2vlCnA3W72tMVLiJRRLlcQVL0DuOH1B4AsLp29EdZvCgnIgoikanZgWa62O9rIDtyJc", "PEMBXPvifzaGkYH5WaDJhPhqqkgmfCljSP8HqP06PN2pJV8owNcKs3bpjIG", "ovqgqg50tEzqvbVEkjV7KHsfwOE5YlzcXpRY8u1Y4bcsMSqhVl", "lXIK84nTQm4bMw6FXDLLiatFSk6dRWo5ShLdc5", "A2GNiLim30ZPASDxV5NSrsNPXEmqzDjMq2llp8IrLwKR6d4FbRPdUpVrc7pWRxjyqnK42BYKj", "pIEGUPrOpGk2q4YP4yElzln3o8SyS1zfQBSi32C7q9r8nB7gdMtUZnKy2Nt", "0KlrhOigMjTlZHBvx3ZFpj4NkaXMHqgpkJsr155WQ84pSOiNhBPxj3kB4GQVKXsFB3i", "clCzxW2k6BC1E", "4KwhweuGhAUGquOiIomWHdN7mO4YTEct1jGrLIWbj1oJo0sHwTo", "97enIxJryjcXKEzpi0dXMoJbVhP8ez01P306fMBYUAYgPRHHsEf", "XC1veKD9bPyl6S6LjJjyRpFJdUDCqmhSDuLdRhOcmyUBsOHT56lZM0FjE9BawwCtpF4uy5M5ec55PLB", "wWeQO1hW1c086Y1ZbgD6CCMnwBERNCKRzihK3ZvWRePBXNqwIHFfW5", "slhuss4JZsVnndSLA1dibdjKyR1qRbw5tUS0sZ7srhPbMXwu7fZeV7A20VVNyOwWuZY5OnjqbBa", "NqXs5Yus21y0jzXP6lu1Ev6hTaQgwbOzZpHHonJwj2XpkPVo", "YLRYHlA14S2QovEv9t1ciznBm72JfYj16bv6brb9l5DfWprGuTTne9vEmMckSufL8xYcekg4", "yQOG2oq7", "Sk9UPWmCUhvgpRSlXQJeacLHSWUr66YYXvlqBkJ4B5OhI2Ea55byxwwqIaAC9rrwAQmq03GfW6S", "wzuXYdROY2hDbLmdelalFESnMh8ERpPPt2UtsqbeSVQTq4eH6gmWfAUeD60qKINuukg6xYLUAC73G0", "YElxVgk2x4VTBcb881nheQeXzRP3Enl9eCV", "46Q3dMonIactbV0MoJQRgfUNHctgq9Tqelw9CAEHN7Zg3TSPa6LfcgRJFSZp6FyamynloiOiMBAVv", "ORD1yU6UN7ct6iXToIQl2DPZb2Vi6dsnGyofYt88XuQ5xQoHGIGzAb9YLzz2dzzjsKIcitnuJa0gPTyYseH", "TE68JFPJcpfQvp9g32J", "lrDeo", "WsxW5ESquF9WeXcjvf1umsCloff4FO8Q906f6hFCXziVe", "CNcdQVPsRFiM1RHvsannJsq7YYo7v3tqvYfqz5N5NvsOQB", "i2cf3uoTBNM3aWXDkhCS3UYbzSahk3kA2004", "nrsI3v5BCfk9uOKs47d4NyvGw6p733RSIt6aWzWCylcZR5DIMV12GFXwDwlesKQGvObdF04M", "ElKto5OLQzDQJiOGkjv5ShI9m", "KNG3pmkVgHdrZoOCHZdvi5KEVBnx7eGbkbSk9iN0Z5zfhbrPFO4x4q4viaonerzr8wBxo4a8wZm9HLEi2gj", "MlOPdhKUKPuUt0XL3eyKEj8FzkEOtd1nRhz826zhEKVB5laR0qjkmHLrOFe61pMFW", "Suh4Gwr8EDHr3gqekl7qHNMojQkb1jereAsgsTuukJjzbXC9QpArvnrcqGwKuWbJFO", "AG8G27cWUkXCN5OfxJN2", "v2z4GcrkgKUnWJMq1hVM82v7Epb", "xJIyfQoHZvwZdwGoKqJdbYZguw8oTefLVJnz0Aa8zdfVwzU7MMPpDHA7rQ", "JgOE91QaEfhdEmC9tyX2jjb72BXzMJCsiudG5Fe0IqXCB2t69TQUL1ZqzzsYbaoVsQA", "l3BxmLQq94uNDJWsaQD6xF9uTtxcK4cxM1oaimvHBL0kT", "2JhrxMOByiSKEwCOIdpX2qgaa6J6fQXqIzBkThvM7QhY3bWtSdq8kHik1u6MzPJQsE", "v6d29FgA45aIg5k0dtOL57MkLxqigGKr6sFLfusbJ", "0PLZpCeGPGMidch7zogPN7pbjlj5K8D1Zm8lLN16iOgy0YB9UMLi26QWFhv31qVVVlqhVqyzDUiZhcAPmnLC5NFC7", "3vYKFkhlaMK9KetsfUnZNXByQ3GXpiQ3RgqyqtQFPEiP2RMt4N7Yrf", "CGVUh5GVnaVP8Pg5PxBPjwD0Mb3StmQ", "Ci38Qkf9OQILCCtGsoGuoD7w4Wdq2m2xvv2PhxPa11Pu4quolC5rGYY2dz57KABvy8qeJJwIaCQJmGZLuIAJydT", "z4EFmOgDDsTj7NjJY2g3oTLK2lT8n6sqCSXq1ILk6bjuD7", "C3ggRDV6twjwsif", "3VWLbxR52uehzOQQaHHUUFagHG7iwNa56au9dgkj0dXiplHwbsfqk061DmSIq8rsJpA", "aaS5cxrZgWS5cQ0dPp3tHkiFwdBrpmrCmWxvMOTb", "N3WXhEd47pM0DwNIfgBshadHD2BKAhdOLS5nPbrdQ1nOgsnuChXYxnEeZZ3Ck", "WVHKLMhEy", "6UDxSggUiIFzBcPwWN2TwZaFpTzPqkJoRlx96bHjba3YeIcyY9W3rnGmuudW3Cb05jfhsPQRhTbVX6g1c7YDAt", "zWrHa", "iUfgkoQOCinv099rj6hK2aACtKz", "EHPUGtEA980wR4mzKXcraEdYxp0qqtNSSDeq60uaxw458lM6bFmscebzfiM4rghFm1rT6MA2nvn8DM1RTf6T4Fbbei", "EsRCtH4AiXXhhTeHRfjQk3NnocMxZI4wLEl429EOlTppuEl3yzw7ERRE3BgkbGKeP2cY", "EPwhjiW3qINsvQ9X3yJ878Gt8AW9RIMgYkdydkT5HmEXHUTjsiadzaoouHmTb4ehZ5qoGjSSktOv2Ck", "VginOjDGNLG1Ps7criPUybt1xhMLRLLoElnf", "ZIJs9LBeHZer7U", "w5w0WRJV89lHTqM3EzGKScWalQ0pInSp2vQhmegE", "wje6EasNTMw6rTgmhCC3iJK64LMfVw", "3ku9prKDQybSL8hIm", "5CefZHUz3PE8tQhelivsAXBmWOGfVPJx2tx2Ke41ujMZpH1O3xa4BGn" 
				};

				std::cout << "File: " << path.string() << "\n";

				for (unsigned int i = 0; i < element_mximum; ++i) {

					auto ts = get_random_string(5, 60);
					auto key = prop::make_record(prop::str{ts});

					if (!test.contains(ts)) {
						test[ts] = ts;
						CHECK(bm.has_free_frames());

						REQUIRE(bpt.insert(key_like_type{ key.view() }, as_value_in(ts)));
						auto itr = bpt.find(key_like_type{ key.view() });
						if (itr == bpt.end()) {
							std::cout << "\n\nfailed to find: " << ts << "\n\n";
							bpt.dump();
						}
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

				auto tsize = test.size();
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
					auto ts = get_random_string(5, 90);
					auto key = prop::make_record(prop::str{ t.first });
					REQUIRE(bpt.update(key_like_type{ key.view() }, as_value_in(ts)));
					test[t.first] = ts;
				}

				validate_keys(bpt);
				check_map();

				tsize = test.size();
				for (unsigned int i = 0; i < tsize; ++i) {

					if (i == tsize - 1) {
						std::cout << "";
					}
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
				validate_keys(bpt);
				check_map();

				CHECK(test.size() == 0);
				auto [root, found] = bpt.get_model().get_accessor().load_root();
				CHECK(root == bpt.get_model().get_invalid_node_id());
				CHECK_FALSE(found);
			}
			std::cout << "Result filesize: " <<  mem.blocks_count() << " blocks " 
				<< mem.blocks_count() * mem.block_size() << " bytes\n";
		}
		std::filesystem::remove(path);
	}

	TEST_CASE("custom less") {
		constexpr static const auto small_buffer_size = DEFAULT_BUFFER_SIZE / 6;
		constexpr static const auto element_mximum = 1000;

		memory_block_device mem(small_buffer_size);

		using BM = buffer_manager<memory_block_device>;
		BM bm(mem, 6);
		using model_type = paged::model<memory_block_device, std::uint32_t, string_less>;
		using node_id_type = typename model_type::node_id_type;
		using bpt_type = fulla::bpt::tree<model_type>;

		static std::random_device rd;
		static std::mt19937 gen(rd());

		SUBCASE("Create string -> string") {
			bpt_type bpt(bm);
			std::map<std::string, std::string> test;

			bpt.get_accessor().check_create_root_node();
			bpt.get_model().set_stringifier_callbacks(
				[&](node_id_type id) -> std::string { return id == bpt.get_model().get_invalid_node_id() ? "<null>" : std::to_string(id); },
				[](key_out_type kout) -> std::string { return std::string{ (const char*)kout.key.data(), kout.key.size() }; },
				[](value_out_type vout) -> std::string { return std::string{ (const char*)vout.val.data(), vout.val.size() }; }
			);

			for (int i = 0; i < element_mximum; ++i) {
				auto ts = get_random_string(5, 26);
				if (!test.contains(ts)) {

					bpt.insert(as_key_like(ts), as_value_in(ts), policies::insert::insert);
					test[ts] = ts;
					//std::cout << "\"" << ts << "\", ";
					auto itr = bpt.find(as_key_like(ts));
					if (itr == bpt.end()) {
						std::cout << "\n\nFail to find: " << ts << "\n";
						bpt.dump();
					}
					REQUIRE(itr != bpt.end());
				}
			}
			validate_keys(bpt);
			//bpt.dump();

			while (!test.empty()) {
				auto val = test.begin();
				auto itr = bpt.find(as_key_like(val->first));
				
				CHECK(itr != bpt.end());
				bpt.erase(itr);
				itr = bpt.find(as_key_like(val->first));
				CHECK(itr == bpt.end());

				test.erase(val);
			}
			validate_keys(bpt);
		}

	}
}
