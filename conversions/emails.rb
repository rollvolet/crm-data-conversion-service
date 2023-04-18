def emails_to_triplestore client
  graph = RDF::Graph.new
  timestamp = DateTime.now.strftime("%Y%m%d%H%M%S")

  emails = client.execute("SELECT t.DataID, t.ID as Nummer, t.email, t.email2, t.DataType FROM tblData t")
  count = 0
  emails.each_with_index do |email, i|
    ['email', 'email2'].each do |field|
      if email[field]
        mail_address = RDF::URI("mailto:#{email[field]}".gsub(/\s/, ''))

        uuid = Mu.generate_uuid()
        email_uri = RDF::URI(BASE_URI % { :resource => 'emails', :id => uuid })
        if email['DataType'] == 'KLA'
          vcard_type = 'customers'
          id = email['Nummer']
        end
        if email['DataType'] == 'CON'
          vcard_type = 'contacts'
          id = email['DataID']
        end
        if email['DataType'] == 'GEB'
          vcard_type = 'buildings'
          id = email['DataID']
        end
        vcard_uri = RDF::URI(BASE_URI % { :resource => vcard_type, :id => id })

        graph << RDF.Statement(email_uri, RDF.type, VCARD.Email)
        graph << RDF.Statement(email_uri, MU_CORE.uuid, uuid)
        graph << RDF.Statement(email_uri, VCARD.hasValue, mail_address)
        graph << RDF.Statement(email_uri, VCARD.hasEmail, vcard_uri)

        # Legacy IDs useful for future conversions
        graph << RDF.Statement(vcard_uri, DCT.identifier, email['DataId'].to_s)
        graph << RDF.Statement(vcard_uri, VCARD.hasUID, email['Nummer'].to_s)
      end
    end

    if ((i + 1) % 1000 == 0)
      Mu.log.info "Processed #{i + 1} records. Will write to file"
      write_graph("#{timestamp}-emails-#{i + 1}-sensitive", graph)
      graph = RDF::Graph.new
    end

    count = i
  end

  # Writing last iteration to file
  write_graph("#{timestamp}-emails-#{count + 1}-sensitive", graph)

  Mu.log.info "Generated #{count + 1} emails"
end
