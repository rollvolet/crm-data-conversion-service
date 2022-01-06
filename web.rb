require 'tiny_tds'
require 'linkeddata'

DCT = RDF::Vocabulary.new("http://purl.org/dc/terms/")
SCHEMA = RDF::Vocabulary.new("http://schema.org/")
PROV = RDF::Vocabulary.new("http://www.w3.org/ns/prov#")
CRM = RDF::Vocabulary.new("http://data.rollvolet.be/vocabularies/crm/")
PRICE = RDF::Vocabulary.new("http://data.rollvolet.be/vocabularies/pricing/")

BASE_URI = 'http://data.rollvolet.be/%{resource}/%{id}'
BOOLEAN_DT = RDF::URI('http://mu.semte.ch/vocabularies/typed-literals/boolean')

OUTPUT_FOLDER = '/data'

client = TinyTds::Client.new username: 'sa', password: ENV['SQL_PASSWORD'], host: 'sql-database', database: 'Klanten'
log.info "Connected to SQL database" if client.active?

def fetch_vat_rates
  vat_rate_map = {}
  vat_rates = query("SELECT ?id ?uri FROM <http://mu.semte.ch/graphs/public> WHERE { ?uri a <http://data.rollvolet.be/vocabularies/pricing/VatRate> ; <http://purl.org/dc/terms/identifier> ?id . }")
  vat_rates.each { |solution| vat_rate_map[solution[:id].value] = solution[:uri] }
  log.info "Build VAT rate map #{vat_rate_map.inspect}"
  vat_rate_map
end

def calculation_line_per_offerline client
  graph = RDF::Graph.new

  offerlines = client.execute("SELECT l.Id, l.Amount, l.Currency FROM TblOfferline l")
  count = 0
  offerlines.each_with_index do |offerline, i|
    uuid = generate_uuid()
    calc_line_uri = RDF::URI(BASE_URI % { :resource => 'calculation-lines', :id => uuid })
    offerline_uri = RDF::URI(BASE_URI % { :resource => 'offerlines', :id => offerline['Id'] })
    amount = RDF::Literal.new(BigDecimal(offerline['Amount'].to_s))

    graph << RDF.Statement(calc_line_uri, RDF.type, CRM.CalculationLine)
    graph << RDF.Statement(calc_line_uri, MU_CORE.uuid, uuid)
    graph << RDF.Statement(calc_line_uri, DCT.isPartOf, offerline_uri)
    graph << RDF.Statement(calc_line_uri, SCHEMA.amount, amount)
    graph << RDF.Statement(calc_line_uri, SCHEMA.currency, offerline['Currency'])

    # Legacy IDs useful for future conversions
    graph << RDF.Statement(offerline_uri, DCT.identifier, offerline['Id'].to_s)

    count = i
  end

  log.info "Generated #{count} calculation lines"
  file_path = File.join(OUTPUT_FOLDER, DateTime.now.strftime("%Y%m%d%H%M%S") + "-calculation-lines.ttl")
  log.info "Writing generated data to file #{file_path}"
  RDF::Writer.open(file_path, format: :ttl) { |writer| writer << graph }
end

def offerlines_to_triplestore client
  graph = RDF::Graph.new
  vat_rate_map = fetch_vat_rates()

  offerlines = client.execute("SELECT l.Id, l.OfferId, l.VatRateId, l.Currency, l.Amount, l.SequenceNumber, l.Description FROM TblOfferline l")
  count = 0
  offerlines.each_with_index do |offerline, i|
    uuid = generate_uuid()
    offerline_uri = RDF::URI(BASE_URI % { :resource => 'offerlines', :id => uuid })
    offer_uri = RDF::URI(BASE_URI % { :resource => 'offers', :id => offerline['OfferId'] })
    amount = RDF::Literal.new(BigDecimal(offerline['Amount'].to_s))
    vat_rate = vat_rate_map[offerline['VatRateId'].to_s]

    logger.warn "Cannot find VAT rate for ID #{offerline["VatRateId"]}" if (vat_rate.nil?)

    graph << RDF.Statement(offerline_uri, RDF.type, CRM.Offerline)
    graph << RDF.Statement(offerline_uri, MU_CORE.uuid, uuid)
    graph << RDF.Statement(offerline_uri, SCHEMA.amount, amount)
    graph << RDF.Statement(offerline_uri, SCHEMA.currency, offerline['Currency'])
    graph << RDF.Statement(offerline_uri, DCT.identifier, offerline['Id'].to_s)
    graph << RDF.Statement(offerline_uri, DCT.description, offerline['Description'])
    graph << RDF.Statement(offerline_uri, SCHEMA.position, offerline['SequenceNumber'])
    graph << RDF.Statement(offerline_uri, PRICE.hasVatRate, vat_rate)
    graph << RDF.Statement(offerline_uri, DCT.isPartOf, offer_uri)

    # Legacy IDs useful for future conversions
    graph << RDF.Statement(offer_uri, DCT.identifier, offerline['OfferId'].to_s)

    count = i
  end

  log.info "Generated #{count} offerlines"
  file_path = File.join(OUTPUT_FOLDER, DateTime.now.strftime("%Y%m%d%H%M%S") + "-offerlines.ttl")
  log.info "Writing generated data to file #{file_path}"
  RDF::Writer.open(file_path, format: :ttl) { |writer| writer << graph }
end

def invoicelines_to_triplestore client
  graph = RDF::Graph.new
  vat_rate_map = fetch_vat_rates()

  invoicelines = client.execute("SELECT l.Id, l.OrderId, l.InvoiceId, l.VatRateId, l.Currency, l.Amount, l.SequenceNumber, l.Description FROM TblInvoiceline l")
  count = 0
  invoicelines.each_with_index do |invoiceline, i|
    uuid = generate_uuid()
    invoiceline_uri = RDF::URI(BASE_URI % { :resource => 'invoicelines', :id => uuid })
    order_uri = RDF::URI(BASE_URI % { :resource => 'orders', :id => invoiceline['OrderId'] })
    invoice_uri = RDF::URI(BASE_URI % { :resource => 'invoices', :id => invoiceline['InvoiceId'] })
    amount = RDF::Literal.new(BigDecimal(invoiceline['Amount'].to_s))
    vat_rate = vat_rate_map[invoiceline['VatRateId'].to_s]

    logger.warn "Cannot find VAT rate for ID #{invoiceline["VatRateId"]}" if (vat_rate.nil?)

    graph << RDF.Statement(invoiceline_uri, RDF.type, CRM.Invoiceline)
    graph << RDF.Statement(invoiceline_uri, MU_CORE.uuid, uuid)
    graph << RDF.Statement(invoiceline_uri, SCHEMA.amount, amount)
    graph << RDF.Statement(invoiceline_uri, SCHEMA.currency, invoiceline['Currency'])
    graph << RDF.Statement(invoiceline_uri, DCT.identifier, invoiceline['Id'].to_s)
    graph << RDF.Statement(invoiceline_uri, DCT.description, invoiceline['Description'])
    graph << RDF.Statement(invoiceline_uri, SCHEMA.position, invoiceline['SequenceNumber'])
    graph << RDF.Statement(invoiceline_uri, PRICE.hasVatRate, vat_rate)
    graph << RDF.Statement(invoiceline_uri, PROV.wasDerivedFrom, order_uri)
    graph << RDF.Statement(invoiceline_uri, DCT.isPartOf, invoice_uri)

    # Legacy IDs useful for future conversions
    graph << RDF.Statement(order_uri, DCT.identifier, invoiceline['OrderId'].to_s)
    graph << RDF.Statement(invoice_uri, DCT.identifier, invoiceline['InvoiceId'].to_s)

    count = i
  end

  log.info "Generated #{count} invoicelines"
  file_path = File.join(OUTPUT_FOLDER, DateTime.now.strftime("%Y%m%d%H%M%S") + "-invoicelines.ttl")
  log.info "Writing generated data to file #{file_path}"
  RDF::Writer.open(file_path, format: :ttl) { |writer| writer << graph }
end

post '/legacy-calculation-lines' do
  calculation_line_per_offerline(client)
  status 204
end

post '/offerlines-to-triplestore' do
  offerlines_to_triplestore(client)
  status 204
end

post '/invoicelines-to-triplestore' do
  invoicelines_to_triplestore(client)
  status 204
end
