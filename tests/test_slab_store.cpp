#include "tests.hpp"

#include "fulla/slots/directory.hpp"
#include "fulla/slab_store/store.hpp"
#include "fulla/storage/memory_block_device.hpp"
#include "fulla/page_allocator/base.hpp"

namespace {

	using namespace fulla;

	template <storage::RandomAccessBlockDevice RadT, typename PidT = std::uint32_t>
	struct test_page_allocator final : public page_allocator::base<RadT, PidT> {
		using base_type = page_allocator::base<RadT, PidT>;
		using pid_type = PidT;
		using underlying_device_type = RadT;
		using page_handle = typename base_type::page_handle;

		constexpr static const pid_type invalid_pid = std::numeric_limits<pid_type>::max();

		test_page_allocator(underlying_device_type& device, std::size_t maximum_pages)
			: base_type(device, maximum_pages)
		{
		}

		page_handle allocate() override {
			allocated++;
			return base_type::allocate();
		}
		void destroy(pid_type) override {
			destoyed++;
		}
		std::size_t allocated = 0;
		std::size_t destoyed = 0;
	};

	using byte = fulla::core::byte;
	using byte_view = fulla::core::byte_view;
	using namespace fulla::slots;
	using device_type = fulla::storage::memory_block_device;
	using page_allocator_type = test_page_allocator<device_type>;

    template <std::uint16_t SlabSize>
	using slab_storage_type = fulla::slab_store::store<page_allocator_type, SlabSize>;

	auto get_random_float(float min = 0.0f, float max = 1.0f) {
		static std::mt19937 rng{ std::random_device{}() };
		std::uniform_real_distribution<float> dist(min, max);
		return dist(rng);
	}

	int get_random_int(int min = 0, int max = 100) {
		static std::mt19937 rng{ std::random_device{}() };
		std::uniform_int_distribution<int> dist(min, max);
		return dist(rng);
	}

	auto get_random_name(std::size_t length = 10) {
		static const char charset[] =
			"abcdefghijklmnopqrstuvwxyz"
			"ABCDEFGHIJKLMNOPQRSTUVWXYZ";
		static std::mt19937 rng{ std::random_device{}() };
		static std::uniform_int_distribution<std::size_t> dist(0, sizeof(charset) - 2);
		std::string result;
		result.resize(length);
		for (std::size_t i = 0; i < length; ++i) {
			result[i] = charset[dist(rng)];
		}
		return result;
	}

	struct test_data_struct {
		int id;
		float value;
		char name[16];
		auto operator<=>(const test_data_struct&) const = default;
	};

	template <typename PtrT>
	auto as_ptr(byte_view bv) {
		return reinterpret_cast<const PtrT *>(bv.data());
	}

}

TEST_SUITE("slab storage") {

	constexpr static const int TEST_SLOTS = 10'000;
	
	TEST_CASE("create") {
		device_type device_type(4096);
		page_allocator_type allocator(device_type, 16);
		using slab_allocator_type = slab_storage_type<sizeof(test_data_struct)>;
		slab_allocator_type store(allocator);
		using pid_type = typename slab_allocator_type::pid_type;

		struct stored_entry {
			test_data_struct data;
			pid_type pid;
		};

		std::unordered_map<int, stored_entry> expected_data;

		for (int i = 0; i < TEST_SLOTS; ++i) {
			auto random_name = get_random_name(15);
			test_data_struct next = {
				.id = get_random_int(1, 10000),
				.value = get_random_float(0.0f, 1000.0f),
				.name = {}
			};
			std::memcpy(next.name, random_name.c_str(), sizeof(next.name) - 1);
			
			auto slot = store.allocate();
			REQUIRE(slot.is_valid());

			auto ph = store.fetch(slot.pid());
			REQUIRE(ph.is_valid());

			auto span = ph.rw_span();
			REQUIRE(span.size() == sizeof(test_data_struct));
			std::memcpy(span.data(), &next, span.size());
			expected_data[i] = stored_entry{ 
				.data = next,
				.pid = slot.pid(),
			};
		}

		for (int i = 0; i < TEST_SLOTS; ++i) {
			const auto &it = expected_data[i];
			auto ph = store.fetch(it.pid);
			REQUIRE(ph.is_valid());
			CHECK(*as_ptr<test_data_struct>(ph.ro_span()) == it.data);
		}

		while (!expected_data.empty()) {
			auto random_id = get_random_int(0, expected_data.size() - 1);
			auto it = expected_data.begin();
			std::advance(it, random_id);
			store.destroy(it->second.pid);
			expected_data.erase(it);
		}

		CHECK(allocator.allocated == allocator.destoyed);

		std::cout << std::format("Slab storage test: Allocated: {}, destroyed: {}\n", allocator.allocated, allocator.destoyed);
    }
}
