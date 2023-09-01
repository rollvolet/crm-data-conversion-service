def try_parse_date_string(value)
  if value
    begin
      if value.include? '/'
        if value.split('/').last.length == 2 # 02/03/23
          Date.strptime(value, '%d/%m/%y')
        else # 02/03/2023
          Date.strptime(value, '%d/%m/%Y')
        end
      else
        if value.split('-').last.length == 2
          Date.strptime(value, '%d-%m-%y')
        else
          Date.strptime(value, '%d-%m-%Y')
        end
      end
    rescue Date::Error
      nil
    end
  else
    nil
  end
end

def offers_and_orders_to_triplestore client
  graph = RDF::Graph.new
  timestamp = DateTime.now.strftime("%Y%m%d%H%M%S")
  employee_map = fetch_employees()

  delivery_method_deliver = RDF::URI("http://data.rollvolet.be/concepts/e8ac5c18-628f-435a-ac36-3c6704c3ff19")
  delivery_method_install = RDF::URI("http://data.rollvolet.be/concepts/89db0214-65d1-444d-9c19-a0e772113b8a")
  delivery_method_pickup = RDF::URI("http://data.rollvolet.be/concepts/d6ed14a0-ff4d-4ee0-8b61-9ccc57155ba3")

  offers = client.execute(%{
SELECT o.OfferteID, o.OfferteNr, o.Offertedatum, o.OfferteBedrag, o.BestelDatum, o.BestelTotaal, o.DocumentIntro, o.DocumentOutro, o.DocumentVersion, o.VoorschotNodig, o.Plaatsing, o.ProductKlaar , o.TeLeveren, o.VerwachteDatum , o.VereisteDatum, o.UrenGepland, o.ManGepland, o.AfgeslotenBestelling, o.RedenAfsluiten, o.Besteld, Technicians.Names
FROM tblOfferte o
LEFT JOIN (
  SELECT t.OrderId, STRING_AGG(e.Voornaam, ';') as Names
  FROM TblOrderTechnician t
  INNER JOIN TblPersoneel e ON e.PersoneelId = t.EmployeeId
  GROUP BY t.OrderId
) AS Technicians ON Technicians.OrderId = o.OfferteID
WHERE o.MuntOfferte = 'EUR' AND (o.MuntBestel = 'EUR' OR o.MuntBestel IS NULL)
})

  count = 0
  offers.each_with_index do |offer, i|
    caze = fetch_case_by_order_id offer['OfferteID']
    if caze.nil?
      case_uri = RDF::URI('http://data.rollvolet.be/cases/undefined')
    else
      case_uri = RDF::URI(caze)
    end

    uuid = Mu.generate_uuid()
    offer_uri = RDF::URI(BASE_URI % { :resource => 'offers', :id => uuid })
    graph << RDF.Statement(offer_uri, RDF.type, SCHEMA.Offer)
    graph << RDF.Statement(offer_uri, MU_CORE.uuid, uuid)
    graph << RDF.Statement(offer_uri, DCT.identifier, offer['OfferteID'].to_s)
    graph << RDF.Statement(offer_uri, DCT.issued, offer['Offertedatum'].to_date)
    graph << RDF.Statement(offer_uri, SCHEMA.identifier, offer['OfferteNr'].to_s)
    graph << RDF.Statement(offer_uri, CRM.documentIntro, offer['DocumentIntro'].to_s) if offer['DocumentIntro']
    graph << RDF.Statement(offer_uri, CRM.documentOutro, offer['DocumentOutro'].to_s) if offer['DocumentOutro']
    graph << RDF.Statement(offer_uri, OWL.versionInfo, offer['DocumentVersion'].to_s) if offer['DocumentVersion']
    graph << RDF.Statement(offer_uri, DCT.source, if offer['OfferteBedrag'] then 'Access' else 'RKB' end)
    graph << RDF.Statement(case_uri, DOSSIER['Dossier.bestaatUit'], offer_uri)

    if offer['Besteld']
      order_uuid = Mu.generate_uuid()
      order_uri = RDF::URI(BASE_URI % { :resource => 'orders', :id => order_uuid })
      graph << RDF.Statement(order_uri, RDF.type, P2PO_DOCUMENT['PurchaseOrder'])
      graph << RDF.Statement(order_uri, MU_CORE.uuid, order_uuid)
      graph << RDF.Statement(order_uri, DCT.identifier, offer['OfferteID'].to_s)
      graph << RDF.Statement(order_uri, DCT.issued, offer['BestelDatum'].to_date)
      graph << RDF.Statement(order_uri, TMO.targetTime, try_parse_date_string(offer['VerwachteDatum'])) if offer['VerwachteDatum']
      graph << RDF.Statement(order_uri, TMO.dueDate, try_parse_date_string(offer['VereisteDatum'])) if offer['VereisteDatum']
      graph << RDF.Statement(order_uri, CRM.scheduledNbOfHours, offer['UrenGepland'].to_i) if offer['UrenGepland']
      graph << RDF.Statement(order_uri, CRM.scheduledNbOfPersons, offer['ManGepland'].to_i) if offer['ManGepland']
      requires_deposit = if offer['VoorschotNodig'] then "true" else "false" end
      graph << RDF.Statement(order_uri, CRM.requiresDeposit, RDF::Literal.new(requires_deposit, datatype: RDF::URI("http://mu.semte.ch/vocabularies/typed-literals/boolean")))
      is_finished = if offer['ProductKlaar'] then "true" else "false" end
      graph << RDF.Statement(order_uri, CRM.productFinished, RDF::Literal.new(is_finished, datatype: RDF::URI("http://mu.semte.ch/vocabularies/typed-literals/boolean")))
      graph << RDF.Statement(order_uri, DCT.source, if offer['BestelTotaal'] then 'Access' else 'RKB' end)
      graph << RDF.Statement(case_uri, DOSSIER['Dossier.bestaatUit'], order_uri)

      if offer['Names']
        offer['Names'].split(';').each do |name|
          employee_uri = employee_map[name]
          graph << RDF.Statement(order_uri, CRM.plannedTechnicians, employee_uri) if employee_uri
        end
      end


      # Enrich case with common fields

      if offer['Plaatsing']
        graph << RDF.Statement(case_uri, SCHEMA.deliveryMethod, delivery_method_install)
      elsif offer['TeLeveren']
        graph << RDF.Statement(case_uri, SCHEMA.deliveryMethod, delivery_method_deliver)
      else
        graph << RDF.Statement(case_uri, SCHEMA.deliveryMethod, delivery_method_pickup)
      end
    end

    if ((i + 1) % 1000 == 0)
      Mu.log.info "Processed #{i} records. Will write to file"
      write_graph("#{timestamp}-offers-#{i}-sensitive", graph)
      graph = RDF::Graph.new
    end

    count = i
  end

  # Writing last iteration to file
  write_graph("#{timestamp}-offers-#{count}-sensitive", graph)

  Mu.log.info "Generated #{count} offers"
  remove_old_offer_case_links_sparql_query
end

def remove_old_offer_case_links_sparql_query
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
    ?case ext:offer ?crmUri .
  }
} WHERE {
  GRAPH ?g {
    ?case a dossier:Dossier ;
       dossier:Dossier.bestaatUit ?offer .
    ?offer a schema:Offer .
    ?offer dct:identifier ?crmId .
     BIND(IRI(CONCAT("http://data.rollvolet.be/offers/", ?crmId)) as ?crmUri)
  }
}

;

DELETE {
  GRAPH ?g {
    ?case ext:order ?crmUri .
  }
} WHERE {
  GRAPH ?g {
    ?case a dossier:Dossier ;
       dossier:Dossier.bestaatUit ?order .
    ?order a p2poDocument:PurchaseOrder .
    ?order dct:identifier ?crmId .
     BIND(IRI(CONCAT("http://data.rollvolet.be/orders/", ?crmId)) as ?crmUri)
  }
}

;

DELETE {
  GRAPH <http://mu.semte.ch/graphs/rollvolet> {
     ?line dct:isPartOf ?crmUri .
  }
} INSERT {
  GRAPH <http://mu.semte.ch/graphs/rollvolet> {
    ?line dct:isPartOf ?offer .
  }
} WHERE {
  GRAPH <http://mu.semte.ch/graphs/rollvolet> {
    ?offer a schema:Offer ;
      dct:identifier ?crmId .
    ?crmUri dct:identifier ?crmId .
     FILTER(STRSTARTS(STR(?crmUri), "http://data.rollvolet.be/offers/"))
     FILTER(?offer != ?crmUri)
     ?line dct:isPartOf ?crmUri ; a <http://data.rollvolet.be/vocabularies/crm/Offerline> .
  }
}

;

DELETE {
  GRAPH <http://mu.semte.ch/graphs/rollvolet> {
     ?line prov:wasDerivedFrom ?crmUri .
  }
} INSERT {
  GRAPH <http://mu.semte.ch/graphs/rollvolet> {
    ?line dct:isPartOf ?order .
  }
} WHERE {
  GRAPH <http://mu.semte.ch/graphs/rollvolet> {
    ?order a p2poDocument:PurchaseOrder ;
      dct:identifier ?crmId .
    ?crmUri dct:identifier ?crmId .
     FILTER(STRSTARTS(STR(?crmUri), "http://data.rollvolet.be/orders/"))
     FILTER(?order != ?crmUri)
     ?line prov:wasDerivedFrom ?crmUri ; a <http://data.rollvolet.be/vocabularies/crm/Invoiceline> .
  }
}

;

DELETE {
  GRAPH <http://mu.semte.ch/graphs/rollvolet> {
     ?visit dct:subject ?crmUri .
  }
} INSERT {
  GRAPH <http://mu.semte.ch/graphs/rollvolet> {
     ?visit dct:subject ?order .
  }
} WHERE {
  GRAPH <http://mu.semte.ch/graphs/rollvolet> {
    ?order a p2poDocument:PurchaseOrder ;
      dct:identifier ?crmId .
    BIND(IRI(CONCAT("http://data.rollvolet.be/orders/", ?crmId)) as ?crmUri)
    ?visit dct:subject ?crmUri ; a ncal:Event .
  }
}


  }


  write_query("#{timestamp}-remove-old-offers-and-orders-links-sensitive", q)
end
