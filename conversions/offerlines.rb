def calculation_line_per_offerline client
  graph = RDF::Graph.new

  offerlines = client.execute("SELECT l.Id, l.Amount, l.Currency FROM TblOfferline l")
  count = 0
  offerlines.each_with_index do |offerline, i|
    uuid = Mu.generate_uuid()
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

  Mu.log.info "Generated #{count} calculation lines"
  file_path = File.join(OUTPUT_FOLDER, DateTime.now.strftime("%Y%m%d%H%M%S") + "-calculation-lines.ttl")
  Mu.log.info "Writing generated data to file #{file_path}"
  RDF::Writer.open(file_path, format: :ttl) { |writer| writer << graph }
end

def offerlines_to_triplestore client
  graph = RDF::Graph.new
  vat_rate_map = fetch_vat_rates()

  offerlines = client.execute("SELECT l.Id, l.OfferId, l.VatRateId, l.Currency, l.Amount, l.SequenceNumber, l.Description FROM TblOfferline l")
  count = 0
  offerlines.each_with_index do |offerline, i|
    uuid = Mu.generate_uuid()
    offerline_uri = RDF::URI(BASE_URI % { :resource => 'offerlines', :id => uuid })
    offer_uri = RDF::URI(BASE_URI % { :resource => 'offers', :id => offerline['OfferId'] })
    amount = RDF::Literal.new(BigDecimal(offerline['Amount'].to_s))
    vat_rate = vat_rate_map[offerline['VatRateId'].to_s]

    Mu.log.warn "Cannot find VAT rate for ID #{offerline["VatRateId"]}" if (vat_rate.nil?)

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

  Mu.log.info "Generated #{count} offerlines"
  file_path = File.join(OUTPUT_FOLDER, DateTime.now.strftime("%Y%m%d%H%M%S") + "-offerlines.ttl")
  Mu.log.info "Writing generated data to file #{file_path}"
  RDF::Writer.open(file_path, format: :ttl) { |writer| writer << graph }
end
