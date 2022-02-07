def telephones_to_triplestore client
  graph = RDF::Graph.new
  country_map = fetch_countries()
  timestamp = DateTime.now.strftime("%Y%m%d%H%M%S")

  telephones = client.execute("SELECT t.DataId, t.TelTypeId, t.LandId, t.Zonenr, t.Telnr, t.TelMemo, t.Volgorde, c.DataType FROM tblTel t INNER JOIN tblData c ON c.DataID = t.DataId")
  count = 0
  telephones.each_with_index do |telephone, i|
    uuid = Mu.generate_uuid()
    telephone_uri = RDF::URI(BASE_URI % { :resource => 'telephones', :id => uuid })
    vcard_type = 'customers' if telephone['DataType'] == 'KLA'
    vcard_type = 'contacts' if telephone['DataType'] == 'CON'
    vcard_type = 'buildings' if telephone['DataType'] == 'GEB'
    vcard_uri = RDF::URI(BASE_URI % { :resource => vcard_type, :id => telephone['DataId'] })
    tel_number = "#{telephone['Zonenr']}#{telephone['Telnr']}".gsub(/\D/, '')
    tel_type = if telephone['TelTypeId'] == 2 then VCARD.Fax else VCARD.Voice end
    country = country_map[telephone['LandId'].to_s]
    position = if telephone['Volgorde'] then telephone['Volgorde'] else 1 end

    graph << RDF.Statement(telephone_uri, RDF.type, VCARD.Telephone)
    graph << RDF.Statement(telephone_uri, MU_CORE.uuid, uuid)
    graph << RDF.Statement(telephone_uri, VCARD.hasValue, tel_number)
    graph << RDF.Statement(telephone_uri, SCHEMA.position, position)
    graph << RDF.Statement(telephone_uri, VCARD.hasNote, telephone['TelMemo']) if telephone['TelMemo']
    graph << RDF.Statement(telephone_uri, VCARD.hasTelephone, vcard_uri)
    graph << RDF.Statement(telephone_uri, VCARD.hasCountryName, country)
    graph << RDF.Statement(telephone_uri, DCT.type, tel_type)

    # Legacy IDs useful for future conversions
    graph << RDF.Statement(vcard_uri, DCT.identifier, telephone['DataId'].to_s)

    if ((i + 1) % 1000 == 0)
      Mu.log.info "Processed #{i + 1} records. Will write to file"
      write_graph("#{timestamp}-telephones-#{i + 1}-sensitive", graph)
      graph = RDF::Graph.new
    end

    count = i
  end

  # Writing last iteration to file
  write_graph("#{timestamp}-telephones-#{count + 1}-sensitive", graph)

  Mu.log.info "Generated #{count + 1} telephones"
end
