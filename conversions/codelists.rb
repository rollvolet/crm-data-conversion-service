# coding: utf-8
def fetch_vat_rates
  vat_rate_map = {}
  vat_rates = Mu.query("SELECT ?id ?uri FROM <http://mu.semte.ch/graphs/public> WHERE { ?uri a <http://data.rollvolet.be/vocabularies/pricing/VatRate> ; <http://purl.org/dc/terms/identifier> ?id . }")
  vat_rates.each { |solution| vat_rate_map[solution[:id].value] = solution[:uri] }
  Mu.log.info "Build VAT rate map #{vat_rate_map.inspect}"
  vat_rate_map
end

def fetch_countries
  country_map = {}
  countries = Mu.query("SELECT ?id ?uri ?tel_prefix FROM <http://mu.semte.ch/graphs/public> WHERE { ?uri a <http://schema.org/Country> ; <http://purl.org/dc/terms/identifier> ?id . }")
  countries.each { |solution| country_map[solution[:id].value] = solution[:uri] }
  Mu.log.info "Build country map #{country_map.inspect}"
  country_map
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
