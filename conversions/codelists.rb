# coding: utf-8
def fetch_vat_rates
  vat_rate_map = {}
  vat_rates = Mu::query("SELECT ?id ?uri FROM <http://mu.semte.ch/graphs/public> WHERE { ?uri a <http://data.rollvolet.be/vocabularies/pricing/VatRate> ; <http://purl.org/dc/terms/identifier> ?id . }")
  vat_rates.each { |solution| vat_rate_map[solution[:id].value] = solution[:uri] }
  # Mu::log.info "Build VAT rate map #{vat_rate_map.inspect}"
  vat_rate_map
end

def fetch_countries
  country_map = {}
  countries = Mu::query("SELECT ?id ?uri ?tel_prefix FROM <http://mu.semte.ch/graphs/public> WHERE { ?uri a <http://schema.org/Country> ; <http://purl.org/dc/terms/identifier> ?id . }")
  countries.each { |solution| country_map[solution[:id].value] = solution[:uri] }
  # Mu::log.info "Build country map #{country_map.inspect}"
  country_map
end

def fetch_languages
  language_map = {}
  languages = Mu::query("SELECT ?id ?uri FROM <http://mu.semte.ch/graphs/public> WHERE { ?uri a <http://schema.org/Language> ; <http://purl.org/dc/terms/identifier> ?id . }")
  languages.each { |solution| language_map[solution[:id].value] = solution[:uri] }
  # Mu::log.info "Build language map #{language_map.inspect}"
  language_map
end

def fetch_way_of_entries
  way_of_entries_map = {}
  entries = Mu::query("SELECT ?id ?uri FROM <http://mu.semte.ch/graphs/public> WHERE { ?uri a <http://www.w3.org/2004/02/skos/core#Concept> ; <http://www.w3.org/2004/02/skos/core#inScheme> <http://data.rollvolet.be/concept-scheme/9d8087db-4307-4f6d-82f3-a414a5cb2076> ; <http://purl.org/dc/terms/identifier> ?id . }")
  entries.each { |solution| way_of_entries_map[solution[:id].value.to_s] = solution[:uri] }
  # Mu::log.info "Build way of entries map #{way_of_entries_map.inspect}"
  way_of_entries_map
end

def fetch_honorific_prefixes
  hon_prefix_map = {}
  prefixes = Mu::query("SELECT * WHERE { GRAPH <http://mu.semte.ch/graphs/public> { ?s a <http://www.w3.org/2004/02/skos/core#Concept> ; <http://www.w3.org/2004/02/skos/core#inScheme>  <http://data.rollvolet.be/concept-scheme/e3297128-117c-4a73-bed1-08a797554897> ; <http://www.w3.org/2004/02/skos/core#prefLabel> ?label  ; <http://purl.org/dc/terms/identifier> ?id . } }")
  prefixes.each do |solution|
    tuples = hon_prefix_map[solution[:id].value.to_s]
    tuples = {} if tuples.nil?
    tuples[solution[:label].language] = solution[:label].value.to_s
    hon_prefix_map[solution[:id].value.to_s] = tuples
  end
  Mu::log.info "Build hon prefix map #{hon_prefix_map.inspect}"
  hon_prefix_map
end

def fetch_product_units
  {
    'NONE' => { nl: '', fr: '', separator: '' },
    'PIECE' => { nl: 'stuk(s)', fr: 'pièce(s)', separator: ' ' },
    'M' => { nl: 'm', fr: 'm', separator: '' },
    'M2' => { nl: 'm²', fr: 'm²', separator: '' },
    'PAIR' => { nl: 'paar', fr: 'paire(s)', separator: ' ' }
  }
end

def fetch_employees
  employee_map = {}
  employees = Mu::query("SELECT ?id ?uri FROM <http://mu.semte.ch/graphs/rollvolet> WHERE { ?uri a <http://www.w3.org/ns/person#Person> ; <http://xmlns.com/foaf/0.1/firstName> ?id . }")
  employees.each { |solution| employee_map[solution[:id].value] = solution[:uri] }
  # Mu::log.info "Build employee map #{employee_map.inspect}"
  employee_map
end
