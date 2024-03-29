def cases_to_triplestore client
  timestamp = DateTime.now.strftime("%Y%m%d%H%M%S")

  request_cases_to_triplestore client, timestamp
  intervention_cases_to_triplestore client, timestamp
  isolated_invoice_cases_to_triplestore client, timestamp

  rename_existing_cases_sparql_query
end

# Create a case for every request
def request_cases_to_triplestore client, timestamp
  vat_rate_map = fetch_vat_rates()
  graph = RDF::Graph.new

  requests = client.execute(%{
SELECT l.AanvraagID, l.CancellationDate, l.CancellationReason, k.DataID, l.KlantID, l.GebouwID, l.ContactID, c.DataID as ContactId, b.DataID as GebouwId, o.OfferteID as OfferteId, o.Besteld, o.AfgeslotenBestelling, o.RedenAfsluiten, f.FactuurId, o.BtwId, o.Referentie, o.Opmerking, o.Produktiebon
FROM TblAanvraag l
LEFT JOIN tblData k ON l.KlantID = k.ID AND k.DataType = 'KLA'
LEFT JOIN tblData c ON l.ContactID = c.ID AND l.KlantID = c.ParentID AND c.DataType = 'CON'
LEFT JOIN tblData b ON l.GebouwID = b.ID AND l.KlantID = b.ParentID AND b.DataType = 'GEB'
LEFT JOIN tblOfferte o ON o.AanvraagId = l.AanvraagID AND o.MuntOfferte = 'EUR'
LEFT JOIN TblFactuur f ON f.OfferteID = o.OfferteID AND f.MuntEenheid = 'EUR'
})
  count = 0
  requests.each_with_index do |request, i|
    uuid = Mu::generate_uuid()
    case_uri = RDF::URI(BASE_URI % { :resource => 'cases', :id => uuid })

    graph << RDF.Statement(case_uri, RDF.type, DOSSIER.Dossier)
    graph << RDF.Statement(case_uri, MU_CORE.uuid, uuid)
    graph << RDF.Statement(case_uri, DCT.identifier, "AD-#{request['AanvraagID']}")

    structured_identifier_uuid = Mu::generate_uuid()
    structured_identifier_uri = RDF::URI(BASE_URI % { :resource => 'structured-identifiers', :id => structured_identifier_uuid })
    graph << RDF.Statement(structured_identifier_uri, RDF.type, GENERIEK.GestructureerdeIdentificator)
    graph << RDF.Statement(structured_identifier_uri, MU_CORE.uuid, structured_identifier_uuid)
    graph << RDF.Statement(structured_identifier_uri, GENERIEK.naamruimte, 'AD')
    graph << RDF.Statement(structured_identifier_uri, GENERIEK.lokaleIdentificator, "#{request['AanvraagID']}")
    graph << RDF.Statement(case_uri, GENERIEK.gestructureerdeIdentificator, structured_identifier_uri)

    request_uri = RDF::URI(BASE_URI % { :resource => 'requests', :id => request['AanvraagID'].to_s })
    graph << RDF.Statement(case_uri, MU_EXT.request, request_uri)
    graph << RDF.Statement(request_uri, DCT.identifier, request['AanvraagID'].to_s)

    if request['KlantID']
      customer_uri = RDF::URI(BASE_URI % { :resource => 'customers', :id => request['KlantID'].to_s })
      graph << RDF.Statement(case_uri, SCHEMA.customer, customer_uri)
      graph << RDF.Statement(customer_uri, DCT.identifier, request['DataID'].to_s)
      graph << RDF.Statement(customer_uri, VCARD.hasUID, request['KlantID'].to_i)
    end
    if request['ContactId']
      contact_uri = RDF::URI(BASE_URI % { :resource => 'contacts', :id => request['ContactId'].to_s })
      graph << RDF.Statement(case_uri, CRM.contact, contact_uri)
      graph << RDF.Statement(contact_uri, DCT.identifier, request['ContactId'].to_s)
    end
    if request['GebouwId']
      building_uri = RDF::URI(BASE_URI % { :resource => 'buildings', :id => request['GebouwId'].to_s })
      graph << RDF.Statement(case_uri, CRM.building, building_uri)
      graph << RDF.Statement(building_uri, DCT.identifier, request['GebouwId'].to_s)
    end
    if request['OfferteId']
      offer_uri = RDF::URI(BASE_URI % { :resource => 'offers', :id => request['OfferteId'].to_s })
      graph << RDF.Statement(case_uri, MU_EXT.offer, offer_uri)
      graph << RDF.Statement(offer_uri, DCT.identifier, request['OfferteId'].to_s)

      if request['Besteld']
        order_uri = RDF::URI(BASE_URI % { :resource => 'orders', :id => request['OfferteId'].to_s })
        graph << RDF.Statement(case_uri, MU_EXT.order, order_uri)
        graph << RDF.Statement(order_uri, DCT.identifier, request['OfferteId'].to_s)
      end
    end
    if request['FactuurId']
      invoice_uri = RDF::URI(BASE_URI % { :resource => 'invoices', :id => request['FactuurId'].to_s })
      graph << RDF.Statement(case_uri, MU_EXT.invoice, invoice_uri)
      graph << RDF.Statement(invoice_uri, DCT.identifier, request['FactuurId'].to_s)
    end
    if request['BtwId']
      vat_rate = vat_rate_map[request['BtwId'].to_s]
      graph << RDF.Statement(case_uri, P2PO_PRICE.hasVATCategoryCode, vat_rate) if vat_rate
    end
    if request['CancellationDate'] or request['AfgeslotenBestelling']
      activity_uuid = Mu::generate_uuid()
      activity_uri = RDF::URI(BASE_URI % { :resource => 'activities', :id => activity_uuid })
      graph << RDF.Statement(activity_uri, RDF.type, PROV.Activity)
      graph << RDF.Statement(activity_uri, MU_CORE.uuid, activity_uuid)
      graph << RDF.Statement(activity_uri, DCT.type, RDF::URI('http://data.rollvolet.be/concepts/5b0eb3d6-bbfb-449a-88c1-ec23ae341dca'))
      graph << RDF.Statement(activity_uri, PROV.startedAtTime, request['CancellationDate'].to_date) if request['CancellationDate']
      if request['CancellationReason']
        graph << RDF.Statement(activity_uri, DCT.description, request['CancellationReason'])
      elsif request['RedenAfsluiten']
        graph << RDF.Statement(activity_uri, DCT.description, request['RedenAfsluiten'])
      end
      graph << RDF.Statement(case_uri, PROV.wasInvalidatedBy, activity_uri)
      graph << RDF.Statement(case_uri, ADMS.status, RDF::URI('http://data.rollvolet.be/concepts/2ffb1b3c-7932-4369-98ac-37539efd2cbe'))
    else
      graph << RDF.Statement(case_uri, ADMS.status, RDF::URI('http://data.rollvolet.be/concepts/2fb2bd3f-1df3-4c45-94a0-69a6af2ab735'))
    end

    graph << RDF.Statement(case_uri, FRAPO.hasReferenceNumber, request['Referentie']) if request['Referentie']
    graph << RDF.Statement(case_uri, SKOS.comment, request['Opmerking']) if request['Opmerking']
    graph << RDF.Statement(case_uri, CRM.hasProductionTicket, RDF::Literal.new(request['Produktiebon'], datatype: RDF::URI("http://mu.semte.ch/vocabularies/typed-literals/boolean")))

    if ((i + 1) % 1000 == 0)
      Mu::log.info "Processed #{i + 1} records. Will write to file"
      write_graph("#{timestamp}-request-cases-#{i + 1}-sensitive", graph)
      graph = RDF::Graph.new
    end

    count = i
  end

  # Writing last iteration to file
  write_graph("#{timestamp}-request-cases-#{count}-sensitive", graph)

  Mu::log.info "Generated #{count} cases for requests"
end


# Create a case for every intervention
def intervention_cases_to_triplestore client, timestamp
  vat_rate_map = fetch_vat_rates()
  graph = RDF::Graph.new

  interventions = client.execute(%{
SELECT l.Id, k.DataID, l.CustomerId, c.DataID as ContactId, b.DataID as GebouwId, f.FactuurId, f.BtwId, f.Opmerking, f.Referentie, l.CancellationDate, l.CancellationReason
FROM TblIntervention l
LEFT JOIN tblData k ON l.CustomerId = k.ID AND k.DataType = 'KLA'
LEFT JOIN tblData c ON l.ContactId  = c.ID AND l.CustomerId  = c.ParentID AND c.DataType = 'CON'
LEFT JOIN tblData b ON l.BuildingId  = b.ID AND l.CustomerId  = b.ParentID AND b.DataType = 'GEB'
LEFT JOIN TblFactuur f ON f.InterventionId = l.Id  AND f.MuntEenheid = 'EUR'
})
  count = 0
  interventions.each_with_index do |intervention, i|
    uuid = Mu::generate_uuid()
    case_uri = RDF::URI(BASE_URI % { :resource => 'cases', :id => uuid })

    graph << RDF.Statement(case_uri, RDF.type, DOSSIER.Dossier)
    graph << RDF.Statement(case_uri, MU_CORE.uuid, uuid)
    graph << RDF.Statement(case_uri, DCT.identifier, "IR-#{intervention['Id']}")

    structured_identifier_uuid = Mu::generate_uuid()
    structured_identifier_uri = RDF::URI(BASE_URI % { :resource => 'structured-identifiers', :id => structured_identifier_uuid })
    graph << RDF.Statement(structured_identifier_uri, RDF.type, GENERIEK.GestructureerdeIdentificator)
    graph << RDF.Statement(structured_identifier_uri, MU_CORE.uuid, structured_identifier_uuid)
    graph << RDF.Statement(structured_identifier_uri, GENERIEK.naamruimte, 'IR')
    graph << RDF.Statement(structured_identifier_uri, GENERIEK.lokaleIdentificator, "#{intervention['Id']}")
    graph << RDF.Statement(case_uri, GENERIEK.gestructureerdeIdentificator, structured_identifier_uri)

    intervention_uri = RDF::URI(BASE_URI % { :resource => 'interventions', :id => intervention['Id'].to_s })
    graph << RDF.Statement(case_uri, MU_EXT.intervention, intervention_uri)
    graph << RDF.Statement(intervention_uri, DCT.identifier, intervention['Id'].to_s)

    if intervention['CustomerId']
      customer_uri = RDF::URI(BASE_URI % { :resource => 'customers', :id => intervention['CustomerId'].to_s })
      graph << RDF.Statement(case_uri, SCHEMA.customer, customer_uri)
      graph << RDF.Statement(customer_uri, DCT.identifier, intervention['DataID'].to_s)
      graph << RDF.Statement(customer_uri, VCARD.hasUID, intervention['CustomerId'].to_i)
    end
    if intervention['ContactId']
      contact_uri = RDF::URI(BASE_URI % { :resource => 'contacts', :id => intervention['ContactId'].to_s })
      graph << RDF.Statement(case_uri, CRM.contact, contact_uri)
      graph << RDF.Statement(contact_uri, DCT.identifier, intervention['ContactId'].to_s)
    end
    if intervention['GebouwId']
      building_uri = RDF::URI(BASE_URI % { :resource => 'buildings', :id => intervention['GebouwId'].to_s })
      graph << RDF.Statement(case_uri, CRM.building, building_uri)
      graph << RDF.Statement(building_uri, DCT.identifier, intervention['GebouwId'].to_s)
    end
    if intervention['FactuurId']
      invoice_uri = RDF::URI(BASE_URI % { :resource => 'invoices', :id => intervention['FactuurId'].to_s })
      graph << RDF.Statement(case_uri, MU_EXT.invoice, invoice_uri)
      graph << RDF.Statement(invoice_uri, DCT.identifier, intervention['FactuurId'].to_s)
    end
    if intervention['BtwId']
      vat_rate = vat_rate_map[intervention['BtwId'].to_s]
      graph << RDF.Statement(case_uri, P2PO_PRICE.hasVATCategoryCode, vat_rate) if vat_rate
    end
    if intervention['CancellationDate']
      activity_uuid = Mu::generate_uuid()
      activity_uri = RDF::URI(BASE_URI % { :resource => 'activities', :id => activity_uuid })
      graph << RDF.Statement(activity_uri, RDF.type, PROV.Activity)
      graph << RDF.Statement(activity_uri, MU_CORE.uuid, activity_uuid)
      graph << RDF.Statement(activity_uri, DCT.type, RDF::URI('http://data.rollvolet.be/concepts/5b0eb3d6-bbfb-449a-88c1-ec23ae341dca'))
      graph << RDF.Statement(activity_uri, PROV.startedAtTime, intervention['CancellationDate'].to_date)
      graph << RDF.Statement(activity_uri, DCT.description, intervention['CancellationReason']) if intervention['CancellationReason']
      graph << RDF.Statement(case_uri, PROV.wasInvalidatedBy, activity_uri)
      graph << RDF.Statement(case_uri, ADMS.status, RDF::URI('http://data.rollvolet.be/concepts/2ffb1b3c-7932-4369-98ac-37539efd2cbe'))
    else
      graph << RDF.Statement(case_uri, ADMS.status, RDF::URI('http://data.rollvolet.be/concepts/2fb2bd3f-1df3-4c45-94a0-69a6af2ab735'))
    end


    graph << RDF.Statement(case_uri, FRAPO.hasReferenceNumber, intervention['Referentie']) if intervention['Referentie']
    graph << RDF.Statement(case_uri, SKOS.comment, intervention['Opmerking']) if intervention['Opmerking']

    if ((i + 1) % 1000 == 0)
      Mu::log.info "Processed #{i + 1} records. Will write to file"
      write_graph("#{timestamp}-intervention-cases-#{i + 1}-sensitive", graph)
      graph = RDF::Graph.new
    end

    count = i
  end

  # Writing last iteration to file
  write_graph("#{timestamp}-intervention-cases-#{count}-sensitive", graph)

  Mu::log.info "Generated #{count} cases for interventions"
end



# Create a case for every isolated invoice (that is not a deposit invoice)
def isolated_invoice_cases_to_triplestore client, timestamp
  vat_rate_map = fetch_vat_rates()
  graph = RDF::Graph.new

  invoices = client.execute(%{
SELECT l.FactuurId, l.Nummer, l.KlantID, k.DataID, c.DataID as ContactId, b.DataID as GebouwId, l.BtwId, l.Opmerking, l.Referentie
FROM TblFactuur l
LEFT JOIN TblVoorschotFactuur vf ON vf.VoorschotFactuurID = l.FactuurId
LEFT JOIN tblData k ON l.KlantID = k.ID AND k.DataType = 'KLA'
LEFT JOIN tblData c ON l.ContactID  = c.ID AND l.KlantID = c.ParentID AND c.DataType = 'CON'
LEFT JOIN tblData b ON l.GebouwID  = b.ID AND l.KlantID = b.ParentID AND b.DataType = 'GEB'
WHERE l.MuntEenheid = 'EUR' AND l.InterventionId IS NULL AND l.OfferteID IS NULL AND vf.VoorschotId IS NULL
})
  count = 0
  invoices.each_with_index do |invoice, i|
    uuid = Mu::generate_uuid()
    case_uri = RDF::URI(BASE_URI % { :resource => 'cases', :id => uuid })

    graph << RDF.Statement(case_uri, RDF.type, DOSSIER.Dossier)
    graph << RDF.Statement(case_uri, MU_CORE.uuid, uuid)
    graph << RDF.Statement(case_uri, DCT.identifier, "F-#{invoice['Nummer']}")

    structured_identifier_uuid = Mu::generate_uuid()
    structured_identifier_uri = RDF::URI(BASE_URI % { :resource => 'structured-identifiers', :id => structured_identifier_uuid })
    graph << RDF.Statement(structured_identifier_uri, RDF.type, GENERIEK.GestructureerdeIdentificator)
    graph << RDF.Statement(structured_identifier_uri, MU_CORE.uuid, structured_identifier_uuid)
    graph << RDF.Statement(structured_identifier_uri, GENERIEK.naamruimte, 'F')
    graph << RDF.Statement(structured_identifier_uri, GENERIEK.lokaleIdentificator, "#{invoice['Nummer']}")
    graph << RDF.Statement(case_uri, GENERIEK.gestructureerdeIdentificator, structured_identifier_uri)

    invoice_uri = RDF::URI(BASE_URI % { :resource => 'invoices', :id => invoice['FactuurId'].to_s })
    graph << RDF.Statement(case_uri, MU_EXT.invoice, invoice_uri)
    graph << RDF.Statement(invoice_uri, DCT.identifier, invoice['FactuurId'].to_s)

    if invoice['KlantID']
      customer_uri = RDF::URI(BASE_URI % { :resource => 'customers', :id => invoice['KlantID'].to_s })
      graph << RDF.Statement(case_uri, SCHEMA.customer, customer_uri)
      graph << RDF.Statement(customer_uri, DCT.identifier, invoice['DataID'].to_s)
      graph << RDF.Statement(customer_uri, VCARD.hasUID, invoice['KlantID'].to_i)
    end
    if invoice['ContactId']
      contact_uri = RDF::URI(BASE_URI % { :resource => 'contacts', :id => invoice['ContactId'].to_s })
      graph << RDF.Statement(case_uri, CRM.contact, contact_uri)
      graph << RDF.Statement(contact_uri, DCT.identifier, invoice['ContactId'].to_s)
    end
    if invoice['GebouwId']
      building_uri = RDF::URI(BASE_URI % { :resource => 'buildings', :id => invoice['GebouwId'].to_s })
      graph << RDF.Statement(case_uri, CRM.building, building_uri)
      graph << RDF.Statement(building_uri, DCT.identifier, invoice['GebouwId'].to_s)
    end
    if invoice['BtwId']
      vat_rate = vat_rate_map[invoice['BtwId'].to_s]
      graph << RDF.Statement(case_uri, P2PO_PRICE.hasVATCategoryCode, vat_rate) if vat_rate
    end

    graph << RDF.Statement(case_uri, FRAPO.hasReferenceNumber, invoice['Referentie']) if invoice['Referentie']
    graph << RDF.Statement(case_uri, SKOS.comment, invoice['Opmerking']) if invoice['Opmerking']

    if ((i + 1) % 1000 == 0)
      Mu::log.info "Processed #{i + 1} records. Will write to file"
      write_graph("#{timestamp}-isolated-invoice-cases-#{i + 1}-sensitive", graph)
      graph = RDF::Graph.new
    end

    count = i
  end

  # Writing last iteration to file
  write_graph("#{timestamp}-isolated-invoice-cases-#{count}-sensitive", graph)

  Mu::log.info "Generated #{count} cases for isolated invoices"
end


def rename_existing_cases_sparql_query
  timestamp = DateTime.now.strftime("%Y%m%d%H%M%S")

  q = %{
PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
PREFIX dct: <http://purl.org/dc/terms/>

DELETE {
  GRAPH ?g {
    ?oldCase dossier:Dossier.bestaatUit ?attachment .
  }
} INSERT {
  GRAPH ?g {
    ?newCase dossier:Dossier.bestaatUit ?attachment .
  }
} WHERE {
  GRAPH ?g {
    ?oldCase a dossier:Dossier ;
      dct:identifier ?caseId ;
      dossier:Dossier.bestaatUit ?attachment .

    ?newCase a dossier:Dossier ;
      dct:identifier ?caseId .

    FILTER (?oldCase != ?newCase)
    FILTER (STRENDS(STR(?oldCase), ?caseId))
  }
}

;

DELETE {
  GRAPH ?g {
    ?oldCase ?p ?o .
  }
} WHERE {
  GRAPH ?g {
    ?oldCase a dossier:Dossier ;
      dct:identifier ?caseId ;
      ?p ?o .

    FILTER (STRENDS(STR(?oldCase), ?caseId))
  }
}
  }

  write_query("#{timestamp}-rename-existing-cases-sensitive", q)
end
