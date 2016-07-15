module.exports = {
	name: 'live_slots_count',
	compute: function (datapart) {
		// console.log(datapart);
		return _.round(datapart.live_available_time / datapart.live_slot_size);
	}
};