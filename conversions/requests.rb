def fetch_case_by_request_id id
  cases = Mu.query("SELECT ?uri FROM <http://mu.semte.ch/graphs/rollvolet> WHERE { ?uri a <https://data.vlaanderen.be/ns/dossier#Dossier> ; <http://mu.semte.ch/vocabularies/ext/request> ?request . ?request <http://purl.org/dc/terms/identifier> #{id.to_s.sparql_escape} . } LIMIT 1")
  if cases.count > 0 then cases[0][:uri].value else nil end
end

def requests_to_triplestore client
  graph = RDF::Graph.new
  timestamp = DateTime.now.strftime("%Y%m%d%H%M%S")
  employee_map = fetch_employees()
  way_of_entries_map = fetch_way_of_entries()

  requests = client.execute(%{
SELECT r.AanvraagID, r.Bezoek, r.Bezoeker, r.Bediende, r.AanmeldingID, r.OriginId, r.Aanvraagdatum, r.Beschrijving, r.Opmerking
FROM TblAanvraag r
WHERE r.Aanvraagdatum > '2000-01-01 00:00:00.000'
})

  count = 0
  requests.each_with_index do |request, i|
    caze = fetch_case_by_request_id request['AanvraagID']
    if caze.nil?
      case_uri = RDF::URI('http://data.rollvolet.be/cases/undefined')
    else
      case_uri = RDF::URI(caze)
    end

    uuid = Mu.generate_uuid()
    request_uri = RDF::URI(BASE_URI % { :resource => 'requests', :id => uuid })
    graph << RDF.Statement(request_uri, RDF.type, CRM.Request)
    graph << RDF.Statement(request_uri, MU_CORE.uuid, uuid)
    graph << RDF.Statement(request_uri, DCT.identifier, request['AanvraagID'].to_s)
    graph << RDF.Statement(request_uri, SCHEMA.identifier, request['AanvraagID'].to_i)
    graph << RDF.Statement(request_uri, DCT.issued, request['Aanvraagdatum'].to_date)
    graph << RDF.Statement(request_uri, DCT.description, request['Beschrijving']) if request['Beschrijving']
    graph << RDF.Statement(request_uri, SKOS.comment, request['Opmerking']) if request['Opmerking']
    graph << RDF.Statement(case_uri, DOSSIER['Dossier.bestaatUit'], request_uri)
    if request['Bezoeker']
      visitor_uri = employee_map[request['Bezoeker']]
      graph << RDF.Statement(request_uri, CRM.visitor, visitor_uri) if visitor_uri
    end
    if request['Bediende']
      employee_uri = employee_map[request['Bediende']]
      graph << RDF.Statement(request_uri, CRM.employee, employee_uri) if employee_uri
    end
    if request['OriginId']
      intervention_uri = RDF::URI(BASE_URI % { :resource => 'interventions', :id => request['OriginId'].to_s })
      graph << RDF.Statement(request_uri, PROV.hadPrimarySource, intervention_uri)
    end
    if request['AanmeldingID']
      way_of_entry_uri = way_of_entries_map[request['AanmeldingID'].to_s]
      graph << RDF.Statement(request_uri, CRM.wayOfEntry, way_of_entry_uri) if way_of_entry_uri
    end

    if ((i + 1) % 1000 == 0)
      Mu.log.info "Processed #{i} records. Will write to file"
      write_graph("#{timestamp}-requests-#{i}-sensitive", graph)
      graph = RDF::Graph.new
    end

    count = i
  end

  # Writing last iteration to file
  write_graph("#{timestamp}-requests-#{count}-sensitive", graph)

  Mu.log.info "Generated #{count} requests"
  remove_old_request_case_links_sparql_query
end

def remove_old_request_case_links_sparql_query
  timestamp = DateTime.now.strftime("%Y%m%d%H%M%S")

  q = %{
PREFIX schema: <http://schema.org/>
PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX p2poDocument: <https://purl.org/p2p-o/document#>
PREFIX p2poInvoice: <https://purl.org/p2p-o/invoice#>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX crm: <http://data.rollvolet.be/vocabularies/crm/>
PREFIX ncal: <http://www.semanticdesktop.org/ontologies/2007/04/02/ncal#>

DELETE {
  GRAPH ?g {
    ?case ext:request ?crmUri .
  }
} WHERE {
  GRAPH ?g {
    ?case a dossier:Dossier ;
       dossier:Dossier.bestaatUit ?request .
    ?request a crm:Request .
    ?request dct:identifier ?crmId .
    BIND(IRI(CONCAT("http://data.rollvolet.be/requests/", ?crmId)) as ?crmUri)
  }
}

;

DELETE {
  GRAPH <http://mu.semte.ch/graphs/rollvolet> {
     ?visit dct:subject ?crmUri .
  }
} INSERT {
  GRAPH <http://mu.semte.ch/graphs/rollvolet> {
     ?visit dct:subject ?request .
  }
} WHERE {
  GRAPH <http://mu.semte.ch/graphs/rollvolet> {
    ?request a crm:Request .
    ?request dct:identifier ?crmId .
    BIND(IRI(CONCAT("http://data.rollvolet.be/requests/", ?crmId)) as ?crmUri)
    ?crmUri dct:identifier ?crmId .
    ?visit a ncal:Event ; dct:subject ?crmUri .
  }
}

;

INSERT {
  GRAPH <http://mu.semte.ch/graphs/rollvolet> {
     ?request dct:source ?source .
  }
} WHERE {
  GRAPH <http://mu.semte.ch/graphs/rollvolet> {
    ?request a crm:Request .
    ?offer a schema:Offer .
    ?case dossier:Dossier.bestaatUit ?request, ?offer .
    ?offer dct:source ?source .
  }
}

  }


  write_query("#{timestamp}-remove-old-request-links-sensitive", q)
end
