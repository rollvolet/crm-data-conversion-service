def fetch_case_by_order_id id
  cases = Mu.query("SELECT ?uri FROM <http://mu.semte.ch/graphs/rollvolet> WHERE { ?uri a <https://data.vlaanderen.be/ns/dossier#Dossier> ; <http://mu.semte.ch/vocabularies/ext/order> ?order . ?order <http://purl.org/dc/terms/identifier> #{id.to_s.sparql_escape} . } LIMIT 1")
  if cases.count > 0 then cases[0][:uri].value else nil end
end

def fetch_case_by_invoice_id id
  cases = Mu.query("SELECT ?uri FROM <http://mu.semte.ch/graphs/rollvolet> WHERE { ?uri a <https://data.vlaanderen.be/ns/dossier#Dossier> ; <http://mu.semte.ch/vocabularies/ext/invoice> ?invoice . ?invoice <http://purl.org/dc/terms/identifier> #{id.to_s.sparql_escape} . } LIMIT 1")
  if cases.count > 0 then cases[0][:uri].value else nil end
end

def fetch_case_by_offer_id id
  cases = Mu.query("SELECT ?uri FROM <http://mu.semte.ch/graphs/rollvolet> WHERE { ?uri a <https://data.vlaanderen.be/ns/dossier#Dossier> ; <http://mu.semte.ch/vocabularies/ext/offer> ?offer . ?offer <http://purl.org/dc/terms/identifier> #{id.to_s.sparql_escape} . } LIMIT 1")
  if cases.count > 0 then cases[0][:uri].value else nil end
end

def fetch_telephones_by_customer_id id
  telephones = Mu.query("SELECT ?uri FROM <http://mu.semte.ch/graphs/rollvolet> WHERE { ?uri a <http://www.w3.org/2006/vcard/ns#Telephone> ; <http://www.w3.org/2006/vcard/ns#hasTelephone> ?customer . ?customer <http://purl.org/dc/terms/identifier> #{id.to_s.sparql_escape} . } LIMIT 1")
  telephones.map { |t| t[:uri].value }
end

def link_snapshots invoice, invoice_uri, graph, country_map, language_map
  # Link customer snapshot
  customer_uri = RDF::URI(BASE_URI % { :resource => 'customers', :id => invoice['KlantNummer'].to_s })
  customer_snap_uuid = Mu.generate_uuid()
  customer_snap_uri = RDF::URI(BASE_URI % { :resource => 'customer-snapshots', :id => customer_snap_uuid })
  customer_snap_type = if invoice['KlantFirma'] then VCARD.Organization else VCARD.Individual end
  name_segments = ['KlantPrefix', 'KlantNaam', 'KlantSuffix']
  if invoice['PrintVoor']
    name_segments.unshift('Aanspreking')
  else
    name_segments.push('Aanspreking')
  end
  customer_snap_name = name_segments.map { |f| invoice[f] }.filter { |v| v }.join " "
  graph << RDF.Statement(customer_snap_uri, RDF.type, CRM.CustomerSnapshot)
  graph << RDF.Statement(customer_snap_uri, MU_CORE.uuid, customer_snap_uuid)
  graph << RDF.Statement(customer_snap_uri, DCT.type, customer_snap_type)
  graph << RDF.Statement(customer_snap_uri, VCARD.hasUID, invoice['KlantNummer'].to_i)
  graph << RDF.Statement(customer_snap_uri, VCARD.hasFN, customer_snap_name)
  graph << RDF.Statement(customer_snap_uri, SCHEMA.vatID, invoice['KlantBTWNummer'].gsub(/\W/, '')) if invoice['KlantBTWNummer']
  graph << RDF.Statement(customer_snap_uri, DCT.created, invoice['Datum'])
  graph << RDF.Statement(customer_snap_uri, VCARD.hasLanguage, language_map[invoice['KlantTaalID'].to_s]) if invoice['KlantTaalID']
  graph << RDF.Statement(invoice_uri, P2PO_INVOICE.hasBuyer, customer_snap_uri)
  graph << RDF.Statement(customer_snap_uri, PROV.hadPrimarySource, customer_uri)

  cust_address_uuid = Mu.generate_uuid()
  cust_address_uri = RDF::URI(BASE_URI % { :resource => 'addresses', :id => cust_address_uuid })
  street = ['KlantAdres1', 'KlantAdres2', 'KlantAdres3'].map { |f| invoice[f] }.filter { |v| v }
  country = country_map[invoice['KlantLandId'].to_s]
  graph << RDF.Statement(cust_address_uri, RDF.type, VCARD.Address)
  graph << RDF.Statement(cust_address_uri, MU_CORE.uuid, cust_address_uuid)
  graph << RDF.Statement(cust_address_uri, VCARD.hasStreetAddress, street.join("\n")) if street.size
  graph << RDF.Statement(cust_address_uri, VCARD.hasPostalCode, invoice['KlantPostcode']) if invoice['KlantPostcode']
  graph << RDF.Statement(cust_address_uri, VCARD.hasLocality, invoice['KlantGemeente']) if invoice['KlantGemeente']
  graph << RDF.Statement(cust_address_uri, VCARD.hasCountryName, country) if country
  graph << RDF.Statement(customer_snap_uri, VCARD.hasAddress, cust_address_uri)

  telephones = fetch_telephones_by_customer_id(invoice['KlantNummer'])
  telephones.each do |telephone|
    graph << RDF.Statement(customer_snap_uri, VCARD.hasTelephone, RDF::URI(telephone))
  end

  # Link contact snapshot
  if invoice['ContactId']
    contact_uri = RDF::URI(BASE_URI % { :resource => 'contacts', :id => invoice['ContactId'].to_s })
    contact_snap_uuid = Mu.generate_uuid()
    contact_snap_uri = RDF::URI(BASE_URI % { :resource => 'contact-snapshots', :id => contact_snap_uuid })
    contact_snap_name = ['ContactPrefix', 'ContactNaam', 'ContactSuffix'].map { |f| invoice[f] }.filter { |v| v }.join " "
    graph << RDF.Statement(contact_snap_uri, RDF.type, CRM.ContactSnapshot)
    graph << RDF.Statement(contact_snap_uri, MU_CORE.uuid, contact_snap_uuid)
    graph << RDF.Statement(contact_snap_uri, SCHEMA.position, invoice['ContactId'].to_i)
    graph << RDF.Statement(contact_snap_uri, VCARD.hasFN, contact_snap_name)
    graph << RDF.Statement(contact_snap_uri, DCT.created, invoice['Datum'])
    graph << RDF.Statement(customer_snap_uri, VCARD.hasLanguage, language_map[invoice['ContactTaalID'].to_s]) if invoice['ContactTaalID']
    graph << RDF.Statement(invoice_uri, P2PO_INVOICE.hasBuyerContactPoint, contact_snap_uri)
    graph << RDF.Statement(contact_snap_uri, PROV.hadPrimarySource, contact_uri)

    cont_address_uuid = Mu.generate_uuid()
    cont_address_uri = RDF::URI(BASE_URI % { :resource => 'addresses', :id => cont_address_uuid })
    street = ['ContactAdres1', 'ContactAdres2', 'ContactAdres3'].map { |f| invoice[f] }.filter { |v| v }
    country = country_map[invoice['ContactLandId'].to_s]
    graph << RDF.Statement(cont_address_uri, RDF.type, VCARD.Address)
    graph << RDF.Statement(cont_address_uri, MU_CORE.uuid, cont_address_uuid)
    graph << RDF.Statement(cont_address_uri, VCARD.hasStreetAddress, street.join("\n")) if street.size
    graph << RDF.Statement(cont_address_uri, VCARD.hasPostalCode, invoice['ContactPostcode']) if invoice['ContactPostcode']
    graph << RDF.Statement(cont_address_uri, VCARD.hasLocality, invoice['ContactGemeente']) if invoice['ContactGemeente']
    graph << RDF.Statement(cont_address_uri, VCARD.hasCountryName, country) if country
    graph << RDF.Statement(contact_snap_uri, VCARD.hasAddress, cont_address_uri)
  end

  if invoice['GebouwId']
    # Link building snapshot
    building_uri = RDF::URI(BASE_URI % { :resource => 'buildings', :id => invoice['GebouwId'].to_s })
    building_snap_uuid = Mu.generate_uuid()
    building_snap_uri = RDF::URI(BASE_URI % { :resource => 'building-snapshots', :id => building_snap_uuid })
    building_snap_name = ['GebouwPrefix', 'GebouwNaam', 'GebouwSuffix'].map { |f| invoice[f] }.filter { |v| v }.join " "
    graph << RDF.Statement(building_snap_uri, RDF.type, CRM.BuildingSnapshot)
    graph << RDF.Statement(building_snap_uri, MU_CORE.uuid, building_snap_uuid)
    graph << RDF.Statement(building_snap_uri, SCHEMA.position, invoice['GebouwId'].to_i)
    graph << RDF.Statement(building_snap_uri, VCARD.hasFN, building_snap_name)
    graph << RDF.Statement(building_snap_uri, DCT.created, invoice['Datum'])
    graph << RDF.Statement(invoice_uri, CRM.hasBuyerBuilding, building_snap_uri)
    graph << RDF.Statement(building_snap_uri, PROV.hadPrimarySource, building_uri)

    building_address_uuid = Mu.generate_uuid()
    building_address_uri = RDF::URI(BASE_URI % { :resource => 'addresses', :id => building_address_uuid })
    street = ['GebouwAdres1', 'GebouwAdres2', 'GebouwAdres3'].map { |f| invoice[f] }.filter { |v| v }
    country = country_map[invoice['GebouwLandId'].to_s]
    graph << RDF.Statement(building_address_uri, RDF.type, VCARD.Address)
    graph << RDF.Statement(building_address_uri, MU_CORE.uuid, building_address_uuid)
    graph << RDF.Statement(building_address_uri, VCARD.hasStreetAddress, street.join("\n")) if street.size
    graph << RDF.Statement(building_address_uri, VCARD.hasPostalCode, invoice['GebouwPostcode']) if invoice['GebouwPostcode']
    graph << RDF.Statement(building_address_uri, VCARD.hasLocality, invoice['GebouwGemeente']) if invoice['GebouwGemeente']
    graph << RDF.Statement(building_address_uri, VCARD.hasCountryName, country) if country
    graph << RDF.Statement(building_snap_uri, VCARD.hasAddress, building_address_uri)
  end
end

def invoices_to_triplestore client
  graph = RDF::Graph.new
  vat_rate_map = fetch_vat_rates()
  country_map = fetch_countries()
  language_map = fetch_languages()
  timestamp = DateTime.now.strftime("%Y%m%d%H%M%S")

  invoices = client.execute(%{
SELECT i.FactuurId, i.Nummer, i.CreditNota, i.Datum, i.VervalDag, i.Geboekt, i.BetaalDatum, i.Afgesloten, i.BtwId, i.Bedrag, i.BasisBedrag, i.BTWBedrag, i.MuntEenheid, i.Origin, i.DocumentOutro, i.Referentie, i.Opmerking, i.Produktiebon, i.Attest, i.AttestTerug, COALESCE(i.OfferteID, vf.OfferteID) as OfferteID, i.KlantID as KlantNummer, k.DataID as KlantId, p.Omschrijving as Aanspreking, k.PrintVoor, c.DataID as ContactId, b.DataID as GebouwId, i.KlantFirma, i.KlantNaam, i.KlantPrefix, i.KlantSuffix, i.KlantAdres1, i.KlantAdres2, i.KlantAdres3, i.KlantPostcode, i.KlantGemeente, i.KlantLandId, i.KlantTaalID, i.KlantBTWNummer, i.ContactNaam, i.ContactPrefix, i.ContactSuffix, i.ContactAdres1, i.ContactAdres2, i.ContactAdres3, i.ContactPostcode, i.ContactGemeente, i.ContactLandId, i.ContactTaalID, i.GebouwNaam, i.GebouwPrefix, i.GebouwSuffix, i.GebouwAdres1, i.GebouwAdres2, i.GebouwAdres3, i.GebouwPostcode, i.GebouwGemeente, i.GebouwLandId, vf.VoorschotId, Voorschot.VoorschotTotaal
FROM TblFactuur i
LEFT JOIN TblVoorschotFactuur vf ON vf.VoorschotFactuurID = i.FactuurId
LEFT JOIN tblData k ON i.KlantID = k.ID AND k.DataType = 'KLA'
LEFT JOIN tblData c ON i.ContactID  = c.ID AND i.KlantID = c.ParentID AND c.DataType = 'CON'
LEFT JOIN tblData b ON i.GebouwID  = b.ID AND i.KlantID = b.ParentID AND b.DataType = 'GEB'
LEFT JOIN TblAanspreekTitel p ON k.AanspreekID = p.AanspreekID AND k.TaalID = p.TaalId
LEFT JOIN (
  SELECT tv.OfferteID, SUM(tv.Bedrag) as VoorschotTotaal
  FROM TblVoorschot tv
  WHERE tv.IsVoorschot = 1
  GROUP BY tv.OfferteID
) as Voorschot ON Voorschot.OfferteID = i.OfferteID
WHERE i.MuntEenheid = 'EUR'
})
  count = 0
  invoices.each_with_index do |invoice, i|
    if invoice['OfferteID']
      caze = fetch_case_by_order_id invoice['OfferteID']
    else
      caze = fetch_case_by_invoice_id invoice['FactuurId']
    end
    if caze.nil?
      case_uri = RDF::URI('http://data.rollvolet.be/cases/undefined')
    else
      case_uri = RDF::URI(caze)
    end

    is_deposit_invoice = !invoice['VoorschotId'].nil?
    vat_rate = vat_rate_map[invoice['BtwId'].to_s]

    # Invoice details
    uuid = Mu.generate_uuid()
    if is_deposit_invoice
      invoice_uri = RDF::URI(BASE_URI % { :resource => 'deposit-invoices', :id => uuid })
      graph << RDF.Statement(invoice_uri, RDF.type, P2PO_INVOICE['E-PrePaymentInvoice'])
      graph << RDF.Statement(invoice_uri, SKOS.comment, invoice['Opmerking']) if invoice['Opmerking']
    else
      invoice_uri = RDF::URI(BASE_URI % { :resource => 'invoices', :id => uuid })
      graph << RDF.Statement(invoice_uri, RDF.type, P2PO_INVOICE['E-FinalInvoice'])
    end
    graph << RDF.Statement(invoice_uri, RDF.type, P2PO_DOCUMENT['E-Invoice'])
    graph << RDF.Statement(invoice_uri, MU_CORE.uuid, uuid)
    graph << RDF.Statement(invoice_uri, DCT.identifier, invoice['FactuurId'].to_s)
    graph << RDF.Statement(invoice_uri, DCT.type, RDF::URI(P2PO_INVOICE['E-CreditNote'])) if invoice['CreditNota']
    graph << RDF.Statement(invoice_uri, P2PO_INVOICE.invoiceNumber, invoice['Nummer'].to_i)
    graph << RDF.Statement(invoice_uri, P2PO_INVOICE.dateOfIssue, invoice['Datum'].to_date) if invoice['Datum']
    graph << RDF.Statement(invoice_uri, P2PO_INVOICE.paymentDueDate, invoice['VervalDag'].to_date) if invoice['VervalDag']
    graph << RDF.Statement(invoice_uri, CRM.bookingDate, invoice['Geboekt'].to_date) if invoice['Geboekt']
    graph << RDF.Statement(invoice_uri, CRM.paymentDate, invoice['BetaalDatum'].to_date) if invoice['BetaalDatum']
    graph << RDF.Statement(invoice_uri, P2PO_INVOICE.hasTotalLineNetAmount, RDF::Literal.new(BigDecimal(invoice['BasisBedrag'].to_s))) if invoice['BasisBedrag']
    graph << RDF.Statement(invoice_uri, P2PO_INVOICE.hasTotalDueForPaymentAmount, RDF::Literal.new(BigDecimal(invoice['Bedrag'].to_s))) if invoice['Bedrag']
    graph << RDF.Statement(invoice_uri, P2PO_INVOICE.hasTotalVATAmount, RDF::Literal.new(BigDecimal(invoice['BTWBedrag'].to_s))) if invoice['BTWBedrag']
    graph << RDF.Statement(invoice_uri, SCHEMA.currency, invoice['MuntEenheid'])
    graph << RDF.Statement(invoice_uri, DCT.source, invoice['Origin'] || 'RKB')
    graph << RDF.Statement(invoice_uri, P2PO_INVOICE.paymentTerms, invoice['DocumentOutro']) if invoice['DocumentOutro']

    requires_vat_certificate = if invoice['Attest'] then "true" else "false" end
    has_vat_certificate = if invoice['AttestTerug'] then "true" else "false" end
    graph << RDF.Statement(invoice_uri, CRM.requiresVatCertificate, RDF::Literal.new(requires_vat_certificate, datatype: RDF::URI("http://mu.semte.ch/vocabularies/typed-literals/boolean")))
    graph << RDF.Statement(invoice_uri, CRM.hasVatCertificate, RDF::Literal.new(has_vat_certificate, datatype: RDF::URI("http://mu.semte.ch/vocabularies/typed-literals/boolean")))

    # Enrich case with common fields
    graph << RDF.Statement(case_uri, DOSSIER['Dossier.bestaatUit'], invoice_uri)
    graph << RDF.Statement(case_uri, FRAPO.hasReferenceNumber, invoice['Referentie']) if invoice['Referentie']
    graph << RDF.Statement(case_uri, P2PO_PRICE.hasVATCategoryCode, vat_rate) if vat_rate
    if invoice['Afgesloten']
      activity_uuid = Mu.generate_uuid()
      activity_uri = RDF::URI(BASE_URI % { :resource => 'activities', :id => activity_uuid })
      graph << RDF.Statement(activity_uri, RDF.type, PROV.Activity)
      graph << RDF.Statement(activity_uri, MU_CORE.uuid, activity_uuid)
      graph << RDF.Statement(activity_uri, DCT.type, RDF::URI('http://data.rollvolet.be/concepts/5b0eb3d6-bbfb-449a-88c1-ec23ae341dca'))
      graph << RDF.Statement(activity_uri, PROV.startedAtTime, invoice['Afgesloten'].to_date)
      graph << RDF.Statement(case_uri, PROV.wasInvalidatedBy, activity_uri)
      graph << RDF.Statement(case_uri, ADMS.status, RDF::URI('http://data.rollvolet.be/concepts/2ffb1b3c-7932-4369-98ac-37539efd2cbe'))
    end

    unless is_deposit_invoice
      graph << RDF.Statement(case_uri, SKOS.comment, invoice['Opmerking']) if invoice['Opmerking']
      has_production_ticket = if invoice['Produktiebon'] then "true" else "false" end
      graph << RDF.Statement(case_uri, CRM.hasProductionTicket, RDF::Literal.new(has_production_ticket, datatype: RDF::URI("http://mu.semte.ch/vocabularies/typed-literals/boolean")))

      graph << RDF.Statement(invoice_uri, CRM.paidDeposits, RDF::Literal.new(BigDecimal(invoice['VoorschotTotaal'].to_s))) if invoice['VoorschotTotaal']
    end

    # Link snapshots
    link_snapshots invoice, invoice_uri, graph, country_map, language_map

    # TODO add link between invoice and credit-note in case of credit-note

    if ((i + 1) % 1000 == 0)
      Mu.log.info "Processed #{i} records. Will write to file"
      write_graph("#{timestamp}-invoices-#{i}-sensitive", graph)
      graph = RDF::Graph.new
    end

    count = i
  end

  # Writing last iteration to file
  write_graph("#{timestamp}-invoices-#{count}-sensitive", graph)

  Mu.log.info "Generated #{count} invoices"
  remove_old_invoice_case_links_sparql_query
end

def remove_old_invoice_case_links_sparql_query
  timestamp = DateTime.now.strftime("%Y%m%d%H%M%S")

  q = %{
PREFIX schema: <http://schema.org/>
PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX p2poDocument: <https://purl.org/p2p-o/document#>
PREFIX p2poInvoice: <https://purl.org/p2p-o/invoice#>
PREFIX prov: <http://www.w3.org/ns/prov#>

DELETE {
  GRAPH ?g {
    ?case ext:invoice ?crmUri .
  }
} WHERE {
  GRAPH ?g {
    ?case a dossier:Dossier ;
       dossier:Dossier.bestaatUit ?invoice .
    ?invoice dct:identifier ?crmId .
     BIND(IRI(CONCAT("http://data.rollvolet.be/invoices/", ?crmId)) as ?crmUri)
  }
}

;

INSERT {
  GRAPH ?h {
    ?case ext:invoice ?invoice .
  }
} WHERE {
  GRAPH ?g {
    ?case a dossier:Dossier ;
       dossier:Dossier.bestaatUit ?invoice .
       ?invoice a p2poInvoice:E-FinalInvoice .
  }
  BIND(?g as ?h)
}

;

DELETE {
  GRAPH <http://mu.semte.ch/graphs/rollvolet> {
     ?line dct:isPartOf ?crmUri .
  }
} INSERT {
  GRAPH <http://mu.semte.ch/graphs/rollvolet> {
    ?invoice p2poInvoice:hasInvoiceLine ?line .
  }
} WHERE {
  GRAPH <http://mu.semte.ch/graphs/rollvolet> {
    ?invoice a p2poInvoice:E-FinalInvoice ;
      dct:identifier ?crmId .
    ?crmUri dct:identifier ?crmId .
     FILTER(STRSTARTS(STR(?crmUri), "http://data.rollvolet.be/invoices/"))
     FILTER(?invoice != ?crmUri)
     ?line dct:isPartOf ?crmUri ; a <http://data.rollvolet.be/vocabularies/crm/Invoiceline> .
  }
}

;

DELETE {
  GRAPH <http://mu.semte.ch/graphs/rollvolet> {
     ?work prov:wasInfluencedBy ?crmUri .
  }
} INSERT {
  GRAPH <http://mu.semte.ch/graphs/rollvolet> {
     ?work prov:wasInfluencedBy ?invoice .
  }
} WHERE {
  GRAPH <http://mu.semte.ch/graphs/rollvolet> {
    ?invoice a p2poInvoice:E-FinalInvoice ;
      dct:identifier ?crmId .
    ?crmUri dct:identifier ?crmId .
     FILTER(STRSTARTS(STR(?crmUri), "http://data.rollvolet.be/invoices/"))
     FILTER(?invoice != ?crmUri)
     ?work prov:wasInfluencedBy ?crmUri ; a <http://data.rollvolet.be/vocabularies/crm/TechnicalWork> .
  }
}

;

DELETE {
  GRAPH <http://mu.semte.ch/graphs/rollvolet> {
    ?s p2poInvoice:hasTotalLineNetAmount  ?amount .
  }
} INSERT {
  GRAPH <http://mu.semte.ch/graphs/rollvolet> {
    ?s p2poInvoice:hasTotalLineNetAmount  ?sum .
  }
}
WHERE {
  {
     SELECT DISTINCT ?s ?amount SUM(?lineAmount) as ?sum WHERE {
       GRAPH <http://mu.semte.ch/graphs/rollvolet> {
         ?s a p2poInvoice:E-FinalInvoice ; p2poInvoice:hasTotalLineNetAmount  ?amount .
         ?s p2poInvoice:hasInvoiceLine ?line .
         ?line schema:amount ?lineAmount .
      }
    }
  }
  ?s p2poInvoice:invoiceNumber ?number .
  FILTER (ABS(?amount - ?sum) > 1)
}  ORDER BY ?number
  }


  write_query("#{timestamp}-remove-old-invoice-links-sensitive", q)
end
