module.exports = {
	name: 'prebook_slots_count',
	compute: function (datapart) {
		let part = datapart.prebook_percentage;
		let max_slots = _.floor(_.min([datapart.prebook_available_time, (datapart.live_total_time * part - datapart.reserved)]) / datapart.prebook_slot_size);
		// console.log("MAXSL", max_slots, datapart.prebook_available_time, (datapart.live_total_time * part - datapart.reserved));
		return _.clamp(_.sumBy(datapart.prebook_chunk_mapping, t => (_.floor(t / datapart.prebook_slot_size) - 1)), 0, max_slots);
	}

};