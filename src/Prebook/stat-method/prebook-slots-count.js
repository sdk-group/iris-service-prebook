module.exports = {
	name: 'prebook_slots_count',
	compute: function (data) {
		return _.round(data.prebook_available_time / data.prebook_slot_size);
	}
};