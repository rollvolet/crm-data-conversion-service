def employees_to_triplestore client
  graph = RDF::Graph.new
  timestamp = DateTime.now.strftime("%Y%m%d%H%M%S")
  invalidation_date = DateTime.parse("2022-10-01T00:00:00")

  employees = client.execute("SELECT l.PersoneelId, l.Type, l.PNaam, l.Voornaam, l.Initialen, l.InDienst, l.Aanvragen FROM TblPersoneel l")
  count = 0
  employees.each_with_index do |employee, i|
    uuid = Mu.generate_uuid()
    employee_uri = RDF::URI(BASE_URI % { :resource => 'employees', :id => uuid })

    graph << RDF.Statement(employee_uri, RDF.type, PERSON.Person)
    graph << RDF.Statement(employee_uri, MU_CORE.uuid, uuid)
    graph << RDF.Statement(employee_uri, DCT.identifier, employee['PersoneelId'].to_s)
    graph << RDF.Statement(employee_uri, FOAF.firstName, employee['Voornaam'].to_s)
    graph << RDF.Statement(employee_uri, FOAF.givenName, employee['PNaam'].to_s)
    graph << RDF.Statement(employee_uri, FRAPO.initial, employee['Initialen'].to_s)
    graph << RDF.Statement(employee_uri, PROV.invalidatedAtTime, invalidation_date) unless employee['InDienst']

    type = if employee['Type'] == 1 then RDF::URI "http://data.rollvolet.be/employee-types/18734ab3-b8c4-428c-aca3-beb4d8e0e0ea" else RDF::URI "http://data.rollvolet.be/employee-types/4d6b2df9-e878-4b03-8d3d-801932c8b7f2" end
    graph << RDF.Statement(employee_uri, DCT.type, type)

    if ((i + 1) % 1000 == 0)
      Mu.log.info "Processed #{i} records. Will write to file"
      write_graph("#{timestamp}-employees-#{i}-sensitive", graph)
      graph = RDF::Graph.new
    end

    count = i
  end

  # Writing last iteration to file
  write_graph("#{timestamp}-employees-#{count}-sensitive", graph)

  Mu.log.info "Generated #{count} employees"
end
