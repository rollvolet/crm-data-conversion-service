def invoicelines_to_triplestore client
  graph = RDF::Graph.new
  vat_rate_map = fetch_vat_rates()
  timestamp = DateTime.now.strftime("%Y%m%d%H%M%S")

  invoicelines = client.execute("SELECT l.Id, l.OrderId, l.InvoiceId, l.VatRateId, l.Currency, l.Amount, l.SequenceNumber, l.Description FROM TblInvoiceline l")
  count = 0
  invoicelines.each_with_index do |invoiceline, i|
    uuid = Mu.generate_uuid()
    invoiceline_uri = RDF::URI(BASE_URI % { :resource => 'invoicelines', :id => uuid })
    order_uri = RDF::URI(BASE_URI % { :resource => 'orders', :id => invoiceline['OrderId'] })
    invoice_uri = RDF::URI(BASE_URI % { :resource => 'invoices', :id => invoiceline['InvoiceId'] })
    amount = RDF::Literal.new(BigDecimal(invoiceline['Amount'].to_s))
    vat_rate = vat_rate_map[invoiceline['VatRateId'].to_s]

    Mu.log.warn "Cannot find VAT rate for ID #{invoiceline["VatRateId"]}" if (vat_rate.nil?)

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
      Mu.log.info "Processed #{i} records. Will write to file"
      write_graph("#{timestamp}-invoicelines-#{i}-sensitive", graph)
      graph = RDF::Graph.new
    end

    count = i
  end

  # Writing last iteration to file
  write_graph("#{timestamp}-invoicelines-#{count}-sensitive", graph)

  Mu.log.info "Generated #{count} invoicelines"
end

def supplements_to_triplestore client
  graph = RDF::Graph.new
  vat_rate_map = fetch_vat_rates()
  product_units = fetch_product_units()
  timestamp = DateTime.now.strftime("%Y%m%d%H%M%S")

  query = "SELECT f.MuntEenheid, f.BtwId, f.KlantTaalID, u.Code, s.FactuurExtraID, s.FactuurID, s.Volgnummer, s.Aantal, s.NettoBedrag, s.Omschrijving"
  query += " FROM TblFactuurExtra s"
  query += " INNER JOIN TblFactuur f ON f.FactuurId = s.FactuurID"
  query += " LEFT JOIN TblProductUnit u ON u.Id = s.EenheidId"
  query += " WHERE f.MuntEenheid = 'EUR'"
  supplements = client.execute(query)

  count = 0
  supplements.each_with_index do |supplement, i|
    uuid = Mu.generate_uuid()
    invoiceline_uri = RDF::URI(BASE_URI % { :resource => 'invoicelines', :id => uuid })
    invoice_uri = RDF::URI(BASE_URI % { :resource => 'invoices', :id => supplement['FactuurID'] })
    amount = RDF::Literal.new(BigDecimal((supplement['NettoBedrag'] || 0).to_s))
    vat_rate = vat_rate_map[supplement['BtwId'].to_s]

    Mu.log.warn "Cannot find VAT rate for ID #{supplement['BtwId']}" if (vat_rate.nil?)

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

    if ((i + 1) % 1000 == 0)
      Mu.log.info "Processed #{i + 1} records. Will write to file"
      write_graph("#{timestamp}-supplements-invoicelines-#{i + 1}-sensitive", graph)
      graph = RDF::Graph.new
    end

    count = i
  end

  # Writing last iteration to file
  write_graph("#{timestamp}-invoicelines-#{count + 1}-sensitive", graph)

  Mu.log.info "Generated #{count + 1} invoicelines"
end
