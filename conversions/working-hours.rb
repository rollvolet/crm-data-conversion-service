def working_hours_to_triplestore client
  graph = RDF::Graph.new
  timestamp = DateTime.now.strftime("%Y%m%d%H%M%S")
  employee_map = fetch_employees()

  working_hours = client.execute("SELECT l.ID, l.FactuurId, l.Datum, l.Technieker FROM tblWerkUren l")
  count = 0
  working_hours.each_with_index do |working_hour, i|
    uuid = Mu.generate_uuid()
    activity_uri = RDF::URI(BASE_URI % { :resource => 'technical-working-activities', :id => uuid })
    invoice_uri = RDF::URI(BASE_URI % { :resource => 'invoices', :id => working_hour['FactuurId'] })
    employee_uri = employee_map[working_hour['Technieker']]

    graph << RDF.Statement(activity_uri, RDF.type, CRM.TechnicalWork)
    graph << RDF.Statement(activity_uri, MU_CORE.uuid, uuid)
    graph << RDF.Statement(activity_uri, DCT.identifier, working_hour['ID'].to_s)
    graph << RDF.Statement(activity_uri, PROV.startedAtTime, working_hour['Datum'].to_date) if working_hour['Datum']
    graph << RDF.Statement(activity_uri, PROV.wasInfluencedBy, invoice_uri) if invoice_uri
    graph << RDF.Statement(activity_uri, PROV.wasAssociatedWith, employee_uri) if employee_uri

    # Legacy IDs useful for future conversions
    graph << RDF.Statement(invoice_uri, DCT.identifier, working_hour['FactuurId'].to_s)

    if ((i + 1) % 1000 == 0)
      Mu.log.info "Processed #{i} records. Will write to file"
      write_graph("#{timestamp}-working-hours-#{i}-sensitive", graph)
      graph = RDF::Graph.new
    end

    count = i
  end

  # Writing last iteration to file
  write_graph("#{timestamp}-working-hours-#{count}-sensitive", graph)

  Mu.log.info "Generated #{count} working hours"
end
