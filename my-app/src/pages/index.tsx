import Image from 'next/image'
import { Inter } from 'next/font/google'

const inter = Inter({ subsets: ['latin'] })

import Map from '@/components/map/map';
import { Layers, TileLayer, VectorLayer } from "@/components/map/layers";
import { fromLonLat, get, toLonLat } from 'ol/proj';
import GeoJSON from 'ol/format/GeoJSON';
import { Controls, FullScreenControl } from "@/components/map/controls"
import { osm, vector } from '@/components/map/source';
import { Circle as CircleStyle, Fill, Stroke, Style } from 'ol/style';
import { useEffect, useState } from 'react';

import { Button } from "@/components/ui/button"
import { Filter } from '@/components/filter';
import { Feature } from 'ol';
import { Point } from 'ol/geom';
import VectorSource from 'ol/source/Vector';
import { Cluster } from 'ol/source';

let styles = {
  'MultiPolygon': new Style({
    stroke: new Stroke({
      color: 'blue',
      width: 1,
    }),
    fill: new Fill({
      color: 'rgba(0, 0, 255, 0.1)',
    }),
  }),
};

const data = [
  // {
  //   title: "NORTHBOURNE AVENUE/ANTILL STREET/MOUAT STREET",
  //   coordinates: [-35.24783, 149.13412]
  // },
  // {
  //   title: "NORTHBOURNE AVENUE/ANTILL STREET/MOUAT STREET",
  //   coordinates: [-35.370261,149.111549]
  // },
  {
    title: "NORTHBOURNE AVENUE/ANTILL STREET/MOUAT STREET",
    coordinates: [-35.175699999999999,149.12217999999999]
  }
]

export default function Home() {

  const [center, setCenter] = useState([133.7751, -23.2744]);
  const [zoom, setZoom] = useState(2);
  const [showLayer1, setShowLayer1] = useState(true);
  const [filterValues, setFilterValues] = useState({})
  const [selectedFilters, setFilter] = useState({})

  useEffect(() => {
    async function fetchData() {
      const res = await fetch('https://mkekla07m6.execute-api.ap-southeast-2.amazonaws.com/filters');

      const data = await res.json();

      setFilterValues(data);
    }
    
    fetchData();
  }, [])


  function set(filter: Record<string, string>) {
    setFilter({
      ...selectedFilters,
      ...filter
    })
  }

  async function search(filters) {
    const { year, month, state} = filters;

    const url = `https://mkekla07m6.execute-api.ap-southeast-2.amazonaws.com/search?year=${year}&month=${month}&state=${state}`

    const res = await fetch(url, {
      method: 'POST'
    });

    const data = await res.json();
  }


  const features = new Array();

  data.forEach((item, i) => {
    features[i] = new Feature(new Point(item.coordinates));
  })

  console.log(features);

  const source = new VectorSource({
    features: features,
  });
  
  const clusterSource = new Cluster({
    distance: 10,
    minDistance: 10,
    source: source,
  });

  const styleCache = {};

  return (
    <div>
      <div className="hidden items-start justify-center gap-6 rounded-lg md:grid lg:grid-cols-3">
        <div className="col-span-1 grid items-start gap-6 lg:col-span-2">
          <Map center={fromLonLat(center)} zoom={zoom}>
            <Layers>
              <TileLayer
                source={osm()}
                zIndex={0}
              />

              <VectorLayer
                source={clusterSource}
                style={function (feature) {
                  const size = feature.get('features').length;
                  let style = styleCache[size];
                  if (!style) {
                    style = new Style({
                      image: new CircleStyle({
                        radius: 10,
                        stroke: new Stroke({
                          color: '#fff',
                        }),
                        fill: new Fill({
                          color: '#3399CC',
                        }),
                      }),
                      // text: new Text({
                      //   text: size.toString(),
                      //   fill: new Fill({
                      //     color: '#fff',
                      //   }),
                      // }),
                    });
                    styleCache[size] = style;
                  }
                  return style;
                }}
              />
            </Layers>
            <Controls>
              <FullScreenControl />
            </Controls>
          </Map>
        </div>

        <div className="col-span-1 grid items-start gap-6 lg:col-span-1 p-8">
          <h1>Filters</h1>

          <form onSubmit={(e) => {
              e.preventDefault();
              console.log(selectedFilters)
              search(selectedFilters);
            }}>
            {filterValues.year && 
              <Filter 
                filterName={"Year"}
                setFilter={set} 
                filter={filterValues.year}
              />
            }
            {filterValues.month && 
              <Filter 
                filterName={"Month"} 
                setFilter={set} 
                filter={filterValues.month}
              />
            }
            {filterValues.state && 
              <Filter 
                filterName={"State"} 
                setFilter={set} 
                filter={filterValues.state}
              />
            }

            <Button type="submit" variant="outline">Search</Button>
          </form>
        </div>

      </div>
    </div>
  )
}
