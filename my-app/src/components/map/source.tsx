import * as olSource from "ol/source";
import { Vector as VectorSource } from 'ol/source';

export function osm() {
	return new olSource.OSM();
}

export function vector({ features }) {
	return new VectorSource({
		features
	});
}
