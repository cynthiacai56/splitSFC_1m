from pcsfc.decoder import DecodeMorton2D


def morton_range(bbox, start, body_len, end_len):
    # Initialize
    x_min, x_max, y_min, y_max = bbox[0], bbox[1], bbox[2], bbox[3]

    nbits = body_len + end_len  # head_length + tail_length
    base_units = [0, 1, 2, 3]  # Each slice has 4 sub-slice
    fronts = [(start << nbits) | (base_unit << (nbits - 2)) for base_unit in base_units]
    ranges = []

    # Iterate through all possible Morton code slices, moving two bits at a time
    for i in range(2, body_len, 2):
        full_one_end = (1 << (nbits - i)) - 1  # nbits个1
        overlaps = []
        for slice_min in fronts:
            slice_max = slice_min + full_one_end

            xs_min, ys_min = DecodeMorton2D(slice_min)
            xs_max, ys_max = DecodeMorton2D(slice_max)

            # Fully containment
            if xs_min >= x_min and xs_max <= x_max and ys_min >= y_min and ys_max <= y_max:
                slice_min_lol = (slice_min >> end_len) - (start << body_len)
                slice_max_lol = (slice_max >> end_len) - (start << body_len)
                ranges.append([slice_min_lol, slice_max_lol])
                #print(slice_min, slice_max, 'fully contained')
            # No containment
            elif xs_max < x_min or xs_min > x_max or ys_max < y_min or ys_min > y_max:
                #print(slice_min, slice_max, 'no containment')
                pass
            # Overlap
            else:
                #print(slice_min, slice_max, 'overlaps')
                new_units = [unit << (nbits - i - 2) for unit in base_units]
                for new_unit in new_units:
                    new_slice_min = slice_min | new_unit
                    overlaps.append(new_slice_min)

        fronts = overlaps
        if len(fronts) == 0:
            break

    overlaps_shift = [(key >> end_len) - (start << body_len)for key in overlaps]
    #overlaps_shift =
    return ranges, overlaps_shift