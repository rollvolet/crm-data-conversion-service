def customer_identifiers_to_triplestore client
  graph = RDF::Graph.new
  timestamp = DateTime.now.strftime("%Y%m%d%H%M%S")

  customers = client.execute("SELECT k.DataID, k.ID FROM tblData k WHERE k.DataType = 'KLA'")
  count = 0
  customers.each_with_index do |customer, i|
    customer_uri = RDF::URI(BASE_URI % { :resource => 'customers', :id => customer['ID'] })

    graph << RDF.Statement(customer_uri, VCARD.hasUID, customer['ID'].to_s)
    graph << RDF.Statement(customer_uri, DCT.identifier, customer['DataID'].to_s)

    if ((i + 1) % 10000 == 0)
      Mu.log.info "Processed #{i + 1} records. Will write to file"
      write_graph("#{timestamp}-customer-identifiers-#{i + 1}-sensitive", graph)
      graph = RDF::Graph.new
    end

    count = i
  end

  # Writing last iteration to file
  write_graph("#{timestamp}-customer-identifiers-#{count + 1}-sensitive", graph)

  Mu.log.info "Generated #{count + 1} customer-identifiers"

  fix_customer_uri_on_telephones_sparql_query
end

def fix_customer_uri_on_telephones_sparql_query
  timestamp = DateTime.now.strftime("%Y%m%d%H%M%S")

  q = %{
PREFIX  dct:  <http://purl.org/dc/terms/>
PREFIX  vcard:  <http://www.w3.org/2006/vcard/ns#>

DELETE {
   GRAPH <http://mu.semte.ch/graphs/rollvolet>
   {
      ?tel vcard:hasTelephone ?customer .
   }
} INSERT {
   GRAPH <http://mu.semte.ch/graphs/rollvolet>
   {
       ?tel vcard:hasTelephone ?customerWithNumber .
   }
} WHERE
 {
   GRAPH <http://mu.semte.ch/graphs/rollvolet>
   {
     ?tel a vcard:Telephone ;
        vcard:hasTelephone ?customer .
     FILTER(CONTAINS(STR(?customer), "customers"))
     BIND(SUBSTR(STR(?customer), STRLEN("http://data.rollvolet.be/customers/") + 1) as ?dataId)
     ?customerWithNumber dct:identifier ?dataId .
     FILTER(CONTAINS(STR(?customerWithNumber), "customers"))
   }
 }
}

  write_query("#{timestamp}-fix-customer-uri-on-telephones-sensitive", q)
end
