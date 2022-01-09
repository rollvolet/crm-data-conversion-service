# coding: utf-8
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
ROLLVOLET_GRAPH = 'http://mu.semte.ch/graphs/rollvolet'

def create_sql_client
  client = TinyTds::Client.new username: 'sa', password: ENV['SQL_PASSWORD'], host: 'sql-database', database: 'Klanten'
  log.info "Connected to SQL database" if client.active?
  client
end

def fetch_vat_rates
  vat_rate_map = {}
  vat_rates = query("SELECT ?id ?uri FROM <http://mu.semte.ch/graphs/public> WHERE { ?uri a <http://data.rollvolet.be/vocabularies/pricing/VatRate> ; <http://purl.org/dc/terms/identifier> ?id . }")
  vat_rates.each { |solution| vat_rate_map[solution[:id].value] = solution[:uri] }
  log.info "Build VAT rate map #{vat_rate_map.inspect}"
  vat_rate_map
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

def format_decimal(number)
  if number then sprintf("%0.2f", number).gsub(/(\d)(?=\d{3}+\.)/, '\1 ').gsub(/\./, ',') else '' end
end

def write_graph(filename, graph)
  file_path = File.join(OUTPUT_FOLDER, "#{filename}.ttl")
  log.info "Writing generated data to file #{file_path}"
  RDF::Writer.open(file_path, format: :ttl) { |writer| writer << graph }
  File.open("#{OUTPUT_FOLDER}/#{filename}.graph", "w+") { |f| f.puts(ROLLVOLET_GRAPH) }
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
  timestamp = DateTime.now.strftime("%Y%m%d%H%M%S")

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

    if ((i + 1) % 1000 == 0)
      log.info "Processed #{i} records. Will write to file"
      write_graph("#{timestamp}-invoicelines-#{i}-sensitive", graph)
      graph = RDF::Graph.new
    end

    count = i
  end

  # Writing last iteration to file
  write_graph("#{timestamp}-invoicelines-#{count}-sensitive", graph)

  log.info "Generated #{count} invoicelines"
end

def supplements_to_triplestore client
  graph = RDF::Graph.new
  vat_rate_map = fetch_vat_rates()
  product_units = fetch_product_units()

  query = "SELECT f.MuntEenheid, f.BtwId, f.KlantTaalID, u.Code, s.FactuurExtraID, s.FactuurID, s.Volgnummer, s.Aantal, s.NettoBedrag, s.Omschrijving"
  query += " FROM TblFactuurExtra s"
  query += " INNER JOIN TblFactuur f ON f.FactuurId = s.FactuurID"
  query += " LEFT JOIN TblProductUnit u ON u.Id = s.EenheidId"
  query += " WHERE f.MuntEenheid = 'EUR'"
  supplements = client.execute(query)

  count = 0
  supplements.each_with_index do |supplement, i|
    uuid = generate_uuid()
    invoiceline_uri = RDF::URI(BASE_URI % { :resource => 'invoicelines', :id => uuid })
    invoice_uri = RDF::URI(BASE_URI % { :resource => 'invoices', :id => supplement['FactuurID'] })
    amount = RDF::Literal.new(BigDecimal((supplement['NettoBedrag'] || 0).to_s))
    vat_rate = vat_rate_map[supplement['BtwId'].to_s]

    logger.warn "Cannot find VAT rate for ID #{supplement['BtwId']}" if (vat_rate.nil?)

    nb = supplement['Aantal']
    nb_display = ''
    if nb and nb > 0
      nb_display = if nb % 1 == 0 then nb.floor else format_decimal(nb) end
    end
    unit = product_units[supplement['Code']] || product_units['NONE']
    unit_separator = unit[:separator]
    unit_label = if supplement['KlantTaalID'] == 2 then unit[:fr] else unit[:nl] end

    description = "#{nb_display}#{unit_separator}#{unit_label} #{supplement['Omschrijving'] || ''}".strip

    graph << RDF.Statement(invoiceline_uri, RDF.type, CRM.Invoiceline)
    graph << RDF.Statement(invoiceline_uri, DCT.type, CRM.AccessInvoiceSupplement)
    graph << RDF.Statement(invoiceline_uri, MU_CORE.uuid, uuid)
    graph << RDF.Statement(invoiceline_uri, SCHEMA.amount, amount)
    graph << RDF.Statement(invoiceline_uri, SCHEMA.currency, supplement['MuntEenheid'])
    graph << RDF.Statement(invoiceline_uri, DCT.identifier, supplement['FactuurExtraID'].to_s)
    graph << RDF.Statement(invoiceline_uri, DCT.description, description)
    graph << RDF.Statement(invoiceline_uri, SCHEMA.position, supplement['Volgnummer'])
    graph << RDF.Statement(invoiceline_uri, PRICE.hasVatRate, vat_rate)
    graph << RDF.Statement(invoiceline_uri, DCT.isPartOf, invoice_uri)

    # Legacy IDs useful for future conversions
    graph << RDF.Statement(invoice_uri, DCT.identifier, supplement['FactuurID'].to_s)

    count = i
  end

  log.info "Generated #{count} invoicelines for supplements"
  file_path = File.join(OUTPUT_FOLDER, DateTime.now.strftime("%Y%m%d%H%M%S") + "-supplements-invoicelines.ttl")
  log.info "Writing generated data to file #{file_path}"
  RDF::Writer.open(file_path, format: :ttl) { |writer| writer << graph }
end

post '/legacy-calculation-lines' do
  sql_client = create_sql_client()
  calculation_line_per_offerline(sql_client)
  status 204
end

post '/offerlines-to-triplestore' do
  sql_client = create_sql_client()
  offerlines_to_triplestore(sql_client)
  status 204
end

post '/invoicelines-to-triplestore' do
  sql_client = create_sql_client()
  invoicelines_to_triplestore(sql_client)
  status 204
end

post '/supplements-to-triplestore' do
  sql_client = create_sql_client()
  supplements_to_triplestore(sql_client)
  status 204
end
