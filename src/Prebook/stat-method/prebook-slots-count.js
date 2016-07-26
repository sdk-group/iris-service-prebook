module.exports = {
	name: 'prebook_slots_count',
	compute: function (datapart) {
		return _.clamp(_.sumBy(datapart.prebook_chunk_mapping, t => _.round(t / datapart.prebook_slot_size)) - 1, 0, Infinity);
	}

};