package db

type Filters struct {
	DefaultValue string   `json:"defaultValue"`
	Values       []string `json:"values"`
}

func FilterOptions() map[string]Filters {

	// Query SELECT COUNT(distinct offence_year), offence_year FROM traffic_offences_data_parquet GROUP BY offence_year
	// @todo - these could come from athena.
	yearArray := []string{
		"2023",
		"2022",
		"2021",
		"2020",
		"2019",
		"2018",
		"2017",
		"2016",
		"2015",
		"2014",
		"2013",
		"2012",
		"2011",
		"2010",
	}

	monthArray := []string{"jan", "feb", "march"}

	stateArray := []string{"wa", "nsw", "nt", "qld", "vic"}

	return map[string]Filters{
		"year": {
			DefaultValue: "2023",
			Values:       yearArray,
		},
		"month": {
			DefaultValue: "may",
			Values:       monthArray,
		},
		"state": {
			DefaultValue: "wa",
			Values:       stateArray,
		},
	}

}
