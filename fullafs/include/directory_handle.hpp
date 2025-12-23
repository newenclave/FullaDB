#pragma once

#include "fulla/bpt/tree.hpp"
#include "fulla/bpt/paged/model.hpp"
#include "fs_page_allocator.hpp"
#include "core.hpp"
#include "page_kinds.hpp"
#include "handle_base.hpp"
#include "key_values.hpp"
#include "file_handle.hpp"
#include "directory_storage_handle.hpp"

namespace fullafs {
	
	using fulla::core::word_u16;

	template <fulla::storage::RandomAccessBlockDevice DevT, typename PidT = std::uint32_t>
	class directory_handle {

	public:

		struct directory_descriptor {
			using leaf_metadata_type = fulla::page::empty_metadata;
			using inode_metadata_type = fulla::page::empty_metadata;
			constexpr static const std::uint16_t leaf_kind_value = static_cast<std::uint16_t>(page::kind::directory_leaf);
			constexpr static const std::uint16_t inode_kind_value = static_cast<std::uint16_t>(page::kind::directory_inode);
		};

		using less_type = core::path_string_less;
		using allocator_type = storage::fs_page_allocator<DevT, PidT>;
		using directory_storage_type = directory_storage_handle<DevT, PidT>;
		using device_type = DevT;
		using pid_type = typename allocator_type::pid_type;
		using page_handle = typename allocator_type::page_handle;
		using cpage_view_type = typename allocator_type::cpage_view_type;
		using page_view_type = typename allocator_type::page_view_type;
		using slot_type = std::uint16_t;

		struct root_accessor {

			using root_type = pid_type;

			root_accessor(root_accessor&&) = default;
			root_accessor& operator = (root_accessor&&) = default;

			root_accessor(directory_handle* dir)
				: dir_(dir)
			{}

			bool has_root() const {
				access_handle hdr(dir_->open());
				if (hdr.is_valid()) {
					const auto eroot = hdr.entry_root();
					return dir_->allocator_->valid_id(eroot);
				}
				return false;
			}

			pid_type get_root() {
				access_handle hdr(dir_->open());
				if (hdr.is_valid()) {
					const auto eroot = hdr.entry_root();
					if (dir_->allocator_->valid_id(eroot)) {
						return eroot;
					}
				}
				return dir_->allocator_->invalid_pid;
			}

			void set_root(pid_type pid) {
				access_handle hdr(dir_->open());
				if (hdr.is_valid()) {
					hdr.set_entry_root(pid);
					hdr.mark_dirty();
				}
			}

		private:
			directory_handle* dir_ = nullptr;
		};

		struct access_handle : public storage::handle_base<allocator_type, page::directory_storage> {
			access_handle(page_handle ph, core::byte_span data)
				: storage::handle_base<allocator_type, page::directory_storage>(std::move(ph))
				, slot_(data)
			{}
			access_handle() = default;

			void set_parent(pid_type p) noexcept {
				get_slot()->parent = p;
			}

			pid_type parent() const noexcept {
				return get_slot()->parent;
			}

			void set_entry_root(pid_type val) noexcept {
				get_slot()->entry_root = val;
			}

			pid_type entry_root() const noexcept {
				return get_slot()->entry_root;
			}

			std::size_t total_count() const noexcept {
				return get_slot()->total_entries;
			}

			std::size_t inc_total_count() noexcept {
				get_slot()->total_entries = (get_slot()->total_entries.get() + 1);
				return get_slot()->total_entries;
			}

			std::size_t dec_total_count() noexcept {
				get_slot()->total_entries = (get_slot()->total_entries.get() - 1);
				return get_slot()->total_entries;
			}
			
			auto get_slot() {
				return reinterpret_cast<page::directory_header*>(slot_.data());
			}

			auto get_slot() const {
				return reinterpret_cast<const page::directory_header*>(slot_.data());
			}

			core::byte_span slot_{};
		};

		using model_type = fulla::bpt::paged::model<
			allocator_type,
			less_type,
			root_accessor>;

		using key_like_type = typename model_type::key_like_type;
		using value_in_type = typename model_type::value_in_type;
		using key_out_type = typename model_type::key_out_type;
		using value_out_type = typename model_type::value_out_type;

		using tree_type = fulla::bpt::tree<model_type>;
		using bpt_iterator = typename tree_type::iterator;

		using file_handle_type = file_handle<device_type, pid_type>;

		class directory_entry {
		public:
			directory_entry() = default;

			directory_entry(key_out_type key, value_out_type value, allocator_type* alloc)
				: key_(key.key)
				, value_(value.val)
				, allocator_(alloc)
			{
				parse_entry();
			}

			core::name_type type() const noexcept {
				return entry_type_;
			}

			std::string name() const {
				return std::string(
					reinterpret_cast<const char*>(name_view_.data()),
					name_view_.size()
				);
			}

			fulla::core::byte_view name_view() const noexcept {
				return name_view_;
			}

			const page::entry_descriptor* descriptor() const noexcept {
				if (value_.size() >= sizeof(page::entry_descriptor)) {
					return reinterpret_cast<const page::entry_descriptor*>(value_.data());
				}
				return nullptr;
			}

			pid_type page_id() const noexcept {
				if (auto* desc = descriptor()) {
					return desc->page;
				}
				return allocator_type::invalid_pid;
			}

			slot_type slot_id() const noexcept {
				if (auto* desc = descriptor()) {
					return desc->slot;
				}
				return word_u16::max();
			}

			directory_handle handle() const {
				if ((entry_type_ == core::name_type::directory) && allocator_) {
					auto pid = page_id();
					auto sid = slot_id();
					if (pid != allocator_type::invalid_pid) {
						return directory_handle(pid, sid, *allocator_);
					}
				}
				return {};
			}

			bool is_valid() const noexcept {
				return !key_.empty() && !value_.empty();
			}

			bool is_directory() const noexcept {
				return entry_type_ == core::name_type::directory;
			}

			bool is_file() const noexcept {
				return entry_type_ == core::name_type::file;
			}

		private:
			void parse_entry() {
				if (!key_.empty()) {
					name_view_ = key_;
				}
				if (auto* desc = descriptor()) {
					entry_type_ = static_cast<core::name_type>(desc->kind.get());
				}
			}

			fulla::core::byte_view key_;
			fulla::core::byte_view value_;
			allocator_type* allocator_ = nullptr;

			core::name_type entry_type_ = core::name_type::file;
			fulla::core::byte_view name_view_;
		};

		class iterator {
		public:
			using iterator_category = std::forward_iterator_tag;
			using value_type = directory_entry;
			using difference_type = std::ptrdiff_t;
			using pointer = const directory_entry*;
			using reference = const directory_entry&;

			iterator() = default;

			iterator(bpt_iterator it, allocator_type* alloc)
				: bpt_it_(it)
				, allocator_(alloc)
			{
				if (bpt_it_.is_valid()) {
					update_current_entry();
				}
			}

			reference operator*() const noexcept {
				return current_entry_;
			}

			pointer operator->() const noexcept {
				return &current_entry_;
			}

			iterator& operator++() {
				if (bpt_it_.is_valid()) {
					++bpt_it_;
					if (bpt_it_.is_valid()) {
						update_current_entry();
					}
				}
				return *this;
			}

			iterator operator++(int) {
				iterator tmp = *this;
				++(*this);
				return tmp;
			}

			bool operator==(const iterator& other) const noexcept {
				if (!bpt_it_.is_valid() && !other.bpt_it_.is_valid()) {
					return true;
				}
				if (!bpt_it_.is_valid() || !other.bpt_it_.is_valid()) {
					return false;
				}
				return bpt_it_ == other.bpt_it_;
			}

			bool operator!=(const iterator& other) const noexcept {
				return !(*this == other);
			}

		private:
			void update_current_entry() {
				if (bpt_it_.is_valid()) {
					auto key = bpt_it_->first;
					auto value = bpt_it_->second;
					current_entry_ = directory_entry(key, value, allocator_);
				}
			}

			bpt_iterator bpt_it_;
			allocator_type* allocator_ = nullptr;
			directory_entry current_entry_;
		};

		directory_handle() = default;
		directory_handle(const directory_handle& other)
			: header_pid_(other.header_pid_)
			, header_slot_(other.header_slot_)
			, allocator_(other.allocator_)
			, bpt_(tree_type(*allocator_, fulla::bpt::paged::settings{}, root_accessor(this)))
		{
			bpt_->set_rebalance_policy(fulla::bpt::policies::rebalance::neighbor_share);
		}

		directory_handle& operator = (const directory_handle& other) {
			header_pid_ = other.header_pid_;
			header_slot_ = other.header_slot_;
			allocator_ =  other.allocator_;
			bpt_ = tree_type(*allocator_, fulla::bpt::paged::settings{}, root_accessor(this));
			bpt_->set_rebalance_policy(fulla::bpt::policies::rebalance::neighbor_share);
			return *this;
		}

		directory_handle(directory_handle&& other) noexcept
			: header_pid_(std::move(other.header_pid_))
			, header_slot_(std::move(other.header_slot_))
			, allocator_(other.allocator_)
			, bpt_(tree_type(*allocator_, fulla::bpt::paged::settings{}, root_accessor(this))) 
		{
			bpt_->set_rebalance_policy(fulla::bpt::policies::rebalance::neighbor_share);
		}

		directory_handle& operator = (directory_handle&& other) noexcept {
			header_pid_ = std::move(other.header_pid_);
			header_slot_ = std::move(other.header_slot_);
			allocator_ = std::move(other.allocator_);
			bpt_ = tree_type(*allocator_, fulla::bpt::paged::settings{}, root_accessor(this));
			bpt_->set_rebalance_policy(fulla::bpt::policies::rebalance::neighbor_share);
			return *this;
		}

		directory_handle(pid_type page, std::uint16_t slot, allocator_type& alloc)
			: header_pid_(page)
			, header_slot_(slot)
			, allocator_(&alloc)
			, bpt_(tree_type(*allocator_, fulla::bpt::paged::settings{}, root_accessor(this)))
		{
			bpt_->set_rebalance_policy(fulla::bpt::policies::rebalance::neighbor_share);
		}

		iterator begin() {
			if (is_valid()) {
				return iterator(bpt_->begin(), allocator_);
			}
			return end();
		}

		iterator end() {
			if (is_valid()) {
				return iterator(bpt_->end(), allocator_);
			}
			return iterator();
		}

		iterator find(const std::string& name) {
			if (is_valid()) {
				const auto key = key_like_type{ core::as_byte_view(name) };
				auto itr = bpt_->find(key);
				return iterator(itr, allocator_);
			}
			return end();
		}

		pid_type pid() const noexcept {
			return header_pid_;
		}

		std::uint16_t slot() const noexcept {
			return header_slot_;
		}

		bool is_valid() const noexcept {
			return (allocator_ != nullptr) 
				&& allocator_->valid_id(header_pid_)
				&& (header_slot_ != 0xFFFF)
 				&& bpt_.has_value();
		}
		
		std::size_t total_entries() noexcept {
			if (auto hdl = open()) {
				return hdl.total_count();
			}
			return 0;
		}

		file_handle_type touch(const std::string& name) {
			if (!is_valid()) {
				return {};
			}
			if (auto new_file = file_handle_type::create(allocator_, pid())) {
				auto& bpt = *bpt_;

				const auto full_name = core::make_directory_name(name);
				auto desc_data = core::make_directory_descriptor();
				auto desc = reinterpret_cast<page::entry_descriptor*>(desc_data.data());
				desc->page = new_file.pid();
				desc->kind = static_cast<word_u16::word_type>(core::name_type::file);

				if (bpt.insert(
					key_like_type{ core::as_byte_view(full_name) },
					value_in_type{ core::as_byte_view(desc_data) })) {
					open().inc_total_count();
					return new_file;
				}
			}
			return {};
		}
		
		file_handle_type open_file(const std::string& name) {
			if (!is_valid()) {
				return {};
			}
			auto itr = find(name);
			if (itr != end() && itr->is_file()) {
				return file_handle_type(itr->page_id(), *allocator_);
			}
			return {};
		}
		
		directory_handle mkdir(const std::string& name) {
			// change this all to exception driven error handling
			if (!is_valid()) {
				return {};
			}
			auto new_dir = create(allocator_, pid(), slot());
			if (new_dir.is_valid()) {
				auto& bpt = *bpt_;
				const auto full_name = core::make_directory_name(name);
				auto desc_data = core::make_directory_descriptor();
				auto desc = reinterpret_cast<page::entry_descriptor *>(desc_data.data());
				desc->page = new_dir.pid();
				desc->slot = new_dir.slot();
				desc->kind = static_cast<word_u16::word_type>(core::name_type::directory);
				if (bpt.insert(
					key_like_type{ core::as_byte_view(full_name) }, 
					value_in_type{ core::as_byte_view(desc_data) })) {
					open().inc_total_count();
					return new_dir;
				}
			}
			return {};
		}

		static directory_handle create(allocator_type* allocator, pid_type parent, std::uint16_t parent_slot) {
			directory_storage_type storage(*allocator);

			auto [new_directory, slot_id] = storage.allocate_entry(parent, parent_slot);

			if (new_directory.is_valid()) {
				return directory_handle(new_directory.pid(), slot_id, *allocator);
			}
			return {};
		}

		static directory_handle open_dir(allocator_type* allocator, pid_type pid, std::uint16_t slot) {

			directory_storage_type pstore(*allocator);

			auto [dir, _] = pstore.open_entry(pid, slot);
			if (dir.is_valid()) {
				return directory_handle(pid, slot, *allocator);
			}
			return {};
		}

		static file_handle_type open_file(allocator_type* allocator, pid_type pid) {
			auto fil = allocator->fetch(pid);
			page_view_type pv{ fil.ro_span() };
			if (pv.header().kind == static_cast<std::uint16_t>(page::kind::file_header)) {
				return file_handle_type(pid, *allocator);
			}
			return {};
		}

	private:

		access_handle open() {
			directory_storage_type dstore(*allocator_);

			auto [hdl, data] = dstore.open_entry(header_pid_, header_slot_);
			if (hdl.is_valid()) {
				return access_handle(hdl, data);
			}
			return {};
		}

		template <typename SubheaderT>
		static auto allocate_page(allocator_type *allocator, page::kind k) {
			auto ph = allocator->allocate();
			page_view_type pv{ ph.rw_span() };
			pv.header().init(static_cast<std::uint16_t>(k), allocator->page_size(), sizeof(SubheaderT));
			return ph;
		}

		pid_type header_pid_ = allocator_type::invalid_pid;
		std::uint16_t header_slot_ = word_u16::max();
		allocator_type *allocator_ = nullptr;
		std::optional<tree_type> bpt_;
	};
}