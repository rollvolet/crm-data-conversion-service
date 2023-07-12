def fetch_case_by_intervention_id id
  cases = Mu.query("SELECT ?uri FROM <http://mu.semte.ch/graphs/rollvolet> WHERE { ?uri a <https://data.vlaanderen.be/ns/dossier#Dossier> ; <http://mu.semte.ch/vocabularies/ext/intervention> ?request . ?request <http://purl.org/dc/terms/identifier> #{id.to_s.sparql_escape} . } LIMIT 1")
  if cases.count > 0 then cases[0][:uri].value else nil end
end

def fetch_order_by_crm_id id
  orders = Mu.query("SELECT ?uri FROM <http://mu.semte.ch/graphs/rollvolet> WHERE { ?uri a <https://purl.org/p2p-o/document#PurchaseOrder> ; <http://purl.org/dc/terms/identifier> #{id.to_s.sparql_escape} . } LIMIT 1")
  if orders.count > 0 then orders[0][:uri].value else nil end
end

def interventions_to_triplestore client
  graph = RDF::Graph.new
  timestamp = DateTime.now.strftime("%Y%m%d%H%M%S")
  employee_map = fetch_employees()
  way_of_entries_map = fetch_way_of_entries()

  interventions = client.execute(%{
SELECT i.Id, I.WayOfEntryId, i.OriginId, e.Voornaam, i.Date, i.Description, i.Comment, i.NbOfPersons, Technicians.Names
FROM TblIntervention i
LEFT JOIN TblPersoneel e ON e.PersoneelId = i.EmployeeId
LEFT JOIN (
  SELECT t.InterventionId, STRING_AGG(e.Voornaam, ';') as Names
  FROM TblInterventionTechnician t
  INNER JOIN TblPersoneel e ON e.PersoneelId = t.EmployeeId
  GROUP BY t.InterventionId
) AS Technicians ON Technicians.InterventionId = i.Id
})

  count = 0
  interventions.each_with_index do |intervention, i|
    caze = fetch_case_by_intervention_id intervention['Id']
    if caze.nil?
      case_uri = RDF::URI('http://data.rollvolet.be/cases/undefined')
    else
      case_uri = RDF::URI(caze)
    end

    uuid = Mu.generate_uuid()
    intervention_uri = RDF::URI(BASE_URI % { :resource => 'interventions', :id => uuid })
    graph << RDF.Statement(intervention_uri, RDF.type, CRM.Intervention)
    graph << RDF.Statement(intervention_uri, MU_CORE.uuid, uuid)
    graph << RDF.Statement(intervention_uri, DCT.identifier, intervention['Id'].to_s)
    graph << RDF.Statement(intervention_uri, SCHEMA.identifier, intervention['Id'].to_i)
    graph << RDF.Statement(intervention_uri, DCT.issued, intervention['Date'].to_date)
    graph << RDF.Statement(intervention_uri, DCT.description, intervention['Description']) if intervention['Description']
    graph << RDF.Statement(intervention_uri, SKOS.comment, intervention['Comment']) if intervention['Comment']
    graph << RDF.Statement(intervention_uri, CRM.scheduledNbOfPersons, intervention['NbOfPersons'].to_i) if intervention['NbOfPersons']
    graph << RDF.Statement(intervention_uri, DCT.source, 'RKB')

    graph << RDF.Statement(case_uri, DOSSIER['Dossier.bestaatUit'], intervention_uri)
    if intervention['Voornaam']
      employee_uri = employee_map[intervention['Voornaam']]
      graph << RDF.Statement(intervention_uri, CRM.employee, employee_uri) if employee_uri
    end
    if intervention['OriginId']
      order = fetch_order_by_crm_id(intervention['OriginId'].to_s)
      graph << RDF.Statement(intervention_uri, PROV.hadPrimarySource, RDF::URI(order)) if order
    end
    if intervention['AanmeldingID']
      way_of_entry_uri = way_of_entries_map[intervention['WayOfEntryId'].to_s]
      graph << RDF.Statement(intervention_uri, CRM.wayOfEntry, way_of_entry_uri) if way_of_entry_uri
    end
    if intervention['Names']
      intervention['Names'].split(';').each do |name|
        employee_uri = employee_map[name]
        graph << RDF.Statement(intervention_uri, CRM.plannedTechnicians, employee_uri) if employee_uri
      end
    end

    if ((i + 1) % 1000 == 0)
      Mu.log.info "Processed #{i} records. Will write to file"
      write_graph("#{timestamp}-interventions-#{i}-sensitive", graph)
      graph = RDF::Graph.new
    end

    count = i
  end

  # Writing last iteration to file
  write_graph("#{timestamp}-interventions-#{count}-sensitive", graph)

  Mu.log.info "Generated #{count} interventions"
  remove_old_intervention_case_links_sparql_query
end

def remove_old_intervention_case_links_sparql_query
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
    ?case ext:intervention ?crmUri .
  }
} WHERE {
  GRAPH ?g {
    ?case a dossier:Dossier ;
       dossier:Dossier.bestaatUit ?intervention .
    ?intervention a crm:Intervention .
    ?intervention dct:identifier ?crmId .
    BIND(IRI(CONCAT("http://data.rollvolet.be/interventions/", ?crmId)) as ?crmUri)
  }
}

;

DELETE {
  GRAPH <http://mu.semte.ch/graphs/rollvolet> {
    ?request prov:hadPrimarySource ?crmUri .
  }
} INSERT {
  GRAPH <http://mu.semte.ch/graphs/rollvolet> {
    ?request prov:hadPrimarySource ?intervention .
  }
} WHERE {
  GRAPH <http://mu.semte.ch/graphs/rollvolet> {
    ?request a crm:Request ; prov:hadPrimarySource ?crmUri .
    ?crmUri dct:identifier ?crmId .
    ?intervention a crm:Intervention ;
      dct:identifier ?crmId .
  }
}

;

DELETE {
  GRAPH <http://mu.semte.ch/graphs/rollvolet> {
     ?visit dct:subject ?crmUri .
  }
} INSERT {
  GRAPH <http://mu.semte.ch/graphs/rollvolet> {
     ?visit dct:subject ?intervention .
  }
} WHERE {
  GRAPH <http://mu.semte.ch/graphs/rollvolet> {
    ?intervention a crm:Intervention .
    ?intervention dct:identifier ?crmId .
    BIND(IRI(CONCAT("http://data.rollvolet.be/interventions/", ?crmId)) as ?crmUri)
    ?crmUri dct:identifier ?crmId .
    ?visit a ncal:Event ; dct:subject ?crmUri .
  }
}

  }


  write_query("#{timestamp}-remove-old-intervention-links-sensitive", q)
end
