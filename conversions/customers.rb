def customers_to_triplestore client
  timestamp = DateTime.now.strftime("%Y%m%d%H%M%S")

  customer_table_to_triplestore client, timestamp, 'KLA'
  customer_table_to_triplestore client, timestamp, 'CON'
  customer_table_to_triplestore client, timestamp, 'GEB'

  remove_old_customer_links_sparql_query
  remove_old_contacts_links_sparql_query
  remove_old_buildings_links_sparql_query
end

def link_address graph, record, record_uri, country_map
  address_uuid = Mu::generate_uuid()
  address_uri = RDF::URI(BASE_URI % { :resource => 'addresses', :id => address_uuid })
  street = ['Adres1', 'Adres2', 'Adres3'].map { |f| record[f] }.filter { |v| v }
  country = country_map[record['LandId'].to_s]
  graph << RDF.Statement(address_uri, RDF.type, VCARD.Address)
  graph << RDF.Statement(address_uri, MU_CORE.uuid, address_uuid)
  graph << RDF.Statement(address_uri, VCARD.hasStreetAddress, street.join("\n")) if street.size
  graph << RDF.Statement(address_uri, VCARD.hasPostalCode, record['Postcode']) if record['Postcode']
  graph << RDF.Statement(address_uri, VCARD.hasLocality, record['Gemeente']) if record['Gemeente']
  graph << RDF.Statement(address_uri, VCARD.hasCountryName, country) if country
  graph << RDF.Statement(record_uri, VCARD.hasAddress, address_uri)
end

def customer_table_to_triplestore client, timestamp, scope
  graph = RDF::Graph.new
  country_map = fetch_countries()
  language_map = fetch_languages()
  hon_prefix_map = fetch_honorific_prefixes()
  lang_symbols_map = { '1' => :nl, '2' => :fr, '3' => nil, '4' => :du, '5' => :en }

  records = client.execute(%{
SELECT d.DataID, d.ID, d.ParentID, d.AanspreekID, d.Prefix, d.Naam, d.Suffix, d.Adres1, d.Adres2, d.Adres3, d.Postcode, d.Gemeente, d.TaalID, d.LandId, d.URL, d.PrintPrefix, d.PrintSuffix, d.PrintVoor, d.Opmerking, d.RegistratieDatum, d.Firma, d.BTWNummer, m.Memo, Keywords.Labels
FROM tblData d
LEFT JOIN TblDataMemo m ON m.DataID = d.DataID
LEFT JOIN (
  SELECT t.DataID, STRING_AGG(k.Keyword, ';') as Labels
  FROM TblDataKeyWord t
  INNER JOIN TblKeyWord k ON k.Id = t.KeywordID
  GROUP BY t.DataID
) AS Keywords ON Keywords.DataID = d.DataID
WHERE d.DataType = '#{scope}'
})
  count = 0
  resource_type = if scope == 'KLA' then 'customers' elsif scope == 'CON' then 'contacts' else 'buildings' end

  records.each_with_index do |record, i|
    uuid = Mu::generate_uuid()
    record_uri = RDF::URI(BASE_URI % { :resource => resource_type, :id => uuid })

    if scope == 'KLA'
      graph << RDF.Statement(record_uri, RDF.type, VCARD.VCard)
      graph << RDF.Statement(record_uri, DCT.type, if record['Firma'] then VCARD.Organization else VCARD.Individual end)
      graph << RDF.Statement(record_uri, VCARD.hasUID, record['ID'].to_i)
      graph << RDF.Statement(record_uri, CRM.memo, record['Memo'].to_s) if record['Memo']

      if record['Labels']
        record['Labels'].split(';').each do |name|
          graph << RDF.Statement(record_uri, SCHEMA.keywords, name)
        end
      end

    else
      graph << RDF.Statement(record_uri, RDF.type, if scope == 'CON' then NCO.Contact else GEBOUW.Gebouw end)
      graph << RDF.Statement(record_uri, SCHEMA.position, record['ID'].to_i)
      graph << RDF.Statement(record_uri, CRM.parentId, record['ParentID'].to_s)
    end

    graph << RDF.Statement(record_uri, MU_CORE.uuid, uuid)
    graph << RDF.Statement(record_uri, DCT.identifier, record['DataID'].to_s)

    if record['AanspreekID'] and record['TaalID']
      lang_sym = lang_symbols_map[record['TaalID'].to_s]
      if lang_sym
        hon_prefix = hon_prefix_map[record['AanspreekID'].to_s]
        if hon_prefix
          hon_prefix_lang = hon_prefix[lang_sym]
          graph << RDF.Statement(record_uri, VCARD.hasHonorificPrefix, hon_prefix_lang) if hon_prefix_lang
        end
      end
    end
    graph << RDF.Statement(record_uri, VCARD.hasGivenName, record['Prefix'].to_s) if record['Prefix']
    graph << RDF.Statement(record_uri, VCARD.hasFamilyName, record['Naam'].to_s) if record['Naam']
    graph << RDF.Statement(record_uri, VCARD.hasHonorificSuffix, record['Suffix'].to_s) if record['Suffix']
    graph << RDF.Statement(record_uri, SCHEMA.vatID, record['BTWNummer'].gsub(/\W/, '')) if record['BTWNummer']
    graph << RDF.Statement(record_uri, VCARD.hasUrl, record['URL'].to_s) if record['URL']
    graph << RDF.Statement(record_uri, VCARD.hasNote, record['Opmerking'].to_s) if record['Opmerking']
    graph << RDF.Statement(record_uri, DCT.created, record['RegistratieDatum']) if record['Registratiedatum']
    print_prefix = if record['PrintPrefix'] then "true" else "false" end
    graph << RDF.Statement(record_uri, CRM.printPrefix, RDF::Literal.new(print_prefix, datatype: RDF::URI("http://mu.semte.ch/vocabularies/typed-literals/boolean")))
    print_suffix = if record['PrintSuffix'] then "true" else "false" end
    graph << RDF.Statement(record_uri, CRM.printSuffix, RDF::Literal.new(print_suffix, datatype: RDF::URI("http://mu.semte.ch/vocabularies/typed-literals/boolean")))
    print_front = if record['PrintVoor'] then "true" else "false" end
    graph << RDF.Statement(record_uri, CRM.printSuffixInFront, RDF::Literal.new(print_front, datatype: RDF::URI("http://mu.semte.ch/vocabularies/typed-literals/boolean")))

    graph << RDF.Statement(record_uri, VCARD.hasLanguage, language_map[record['TaalID'].to_s]) if record['TaalID']

    link_address graph, record, record_uri, country_map

    if ((i + 1) % 1000 == 0)
      Mu::log.info "Processed #{i} records. Will write to file"
      write_graph("#{timestamp}-#{resource_type}-#{i}-sensitive", graph)
      graph = RDF::Graph.new
    end

    count = i
  end

  # Writing last iteration to file
  write_graph("#{timestamp}-#{resource_type}-#{count}-sensitive", graph)

  Mu::log.info "Generated #{count} #{resource_type}"
end

def remove_old_customer_links_sparql_query
  timestamp = DateTime.now.strftime("%Y%m%d%H%M%S")

  q = %{
PREFIX schema: <http://schema.org/>
PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX crm: <http://data.rollvolet.be/vocabularies/crm/>
PREFIX vcard: <http://www.w3.org/2006/vcard/ns#>

DELETE {
  GRAPH ?g {
      ?customer vcard:hasUID ?number2.
  }
} WHERE {
  GRAPH ?g {
    ?case a dossier:Dossier ; schema:customer ?customer .
    ?customer vcard:hasUID ?number1 , ?number2.
    FILTER(?number1 != ?number2)
    FILTER(DATATYPE(?number2) = xsd:string)
  }
}

;

DELETE {
  GRAPH ?g {
    ?case schema:customer ?crmUri .
  }
} INSERT {
  GRAPH ?g {
    ?case schema:customer ?customer .
  }
} WHERE {
  GRAPH ?g {
    ?case a dossier:Dossier ; schema:customer ?crmUri .
    ?crmUri vcard:hasUID ?number .
    FILTER(STRSTARTS(STR(?crmUri), "http://data.rollvolet.be/customers/"))
    ?customer a vcard:VCard ;
       vcard:hasUID ?number .
  }
}

;

DELETE {
  GRAPH ?g {
    ?tel vcard:hasTelephone ?crmUri .
  }
} INSERT {
  GRAPH ?g {
    ?customer vcard:hasTelephone ?tel .
  }
} WHERE {
  GRAPH ?g {
    ?tel a vcard:Telephone ; vcard:hasTelephone ?crmUri .
    ?crmUri vcard:hasUID ?number .
    FILTER(STRSTARTS(STR(?crmUri), "http://data.rollvolet.be/customers/"))
    ?customer a vcard:VCard ;
       vcard:hasUID ?number .
  }
}

;

DELETE {
  GRAPH ?g {
    ?email vcard:hasEmail ?crmUri .
  }
} INSERT {
  GRAPH ?g {
    ?customer vcard:hasEmail ?email .
  }
} WHERE {
  GRAPH ?g {
    ?email a vcard:Email ; vcard:hasEmail ?crmUri .
    ?crmUri vcard:hasUID ?number .
    FILTER(STRSTARTS(STR(?crmUri), "http://data.rollvolet.be/customers/"))
    ?customer a vcard:VCard ;
       vcard:hasUID ?number .
  }
}

  }

  write_query("#{timestamp}-remove-old-customer-links-sensitive", q)
end


def remove_old_contacts_links_sparql_query
  timestamp = DateTime.now.strftime("%Y%m%d%H%M%S")

  q = %{
PREFIX schema: <http://schema.org/>
PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX crm: <http://data.rollvolet.be/vocabularies/crm/>
PREFIX vcard: <http://www.w3.org/2006/vcard/ns#>
PREFIX nco: <http://www.semanticdesktop.org/ontologies/2007/03/22/nco#>

DELETE {
  GRAPH ?g {
    ?case crm:contact ?crmUri .
  }
} INSERT {
  GRAPH ?g {
    ?case crm:contact ?contact .
  }
} WHERE {
  GRAPH ?g {
    ?contact a nco:Contact ;
       dct:identifier ?crmId .
    BIND(IRI(CONCAT("http://data.rollvolet.be/contacts/", ?crmId)) as ?crmUri)
    ?contact dct:identifier ?crmId .
    ?case a dossier:Dossier ; crm:contact ?crmUri .
  }
}

;

DELETE {
  GRAPH ?g {
    ?contact crm:parentId ?number .
  }
} INSERT {
  GRAPH ?g {
    ?customer nco:representative ?contact .
  }
} WHERE {
  GRAPH ?g {
    ?customer a vcard:VCard ;
       vcard:hasUID ?number .
    ?contact a nco:Contact ; crm:parentId ?number .
  }
}

;

DELETE {
  GRAPH ?g {
    ?tel vcard:hasTelephone ?crmUri .
  }
} INSERT {
  GRAPH ?g {
    ?contact vcard:hasTelephone ?tel .
  }
} WHERE {
  GRAPH ?g {
    ?tel a vcard:Telephone ; vcard:hasTelephone ?crmUri .
    FILTER(STRSTARTS(STR(?crmUri), "http://data.rollvolet.be/contacts/"))
    BIND(STRAFTER(STR(?crmUri), "http://data.rollvolet.be/contacts/") as ?crmId)
    ?contact a nco:Contact ;
       dct:identifier ?crmId .
  }
}

;

DELETE {
  GRAPH ?g {
    ?email vcard:hasEmail ?crmUri .
  }
} INSERT {
  GRAPH ?g {
    ?contact vcard:hasEmail ?email .
  }
} WHERE {
  GRAPH ?g {
    ?email a vcard:Email ; vcard:hasEmail ?crmUri .
    FILTER(STRSTARTS(STR(?crmUri), "http://data.rollvolet.be/contacts/"))
    BIND(STRAFTER(STR(?crmUri), "http://data.rollvolet.be/contacts/") as ?crmId)
    ?contact a nco:Contact ;
       dct:identifier ?crmId .
  }
}

  }

  write_query("#{timestamp}-remove-old-contacts-links-sensitive", q)
end



def remove_old_buildings_links_sparql_query
  timestamp = DateTime.now.strftime("%Y%m%d%H%M%S")

  q = %{
PREFIX schema: <http://schema.org/>
PREFIX dossier: <https://data.vlaanderen.be/ns/dossier#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX crm: <http://data.rollvolet.be/vocabularies/crm/>
PREFIX vcard: <http://www.w3.org/2006/vcard/ns#>
PREFIX gebouw: <https://data.vlaanderen.be/ns/gebouw#>

DELETE {
  GRAPH ?g {
    ?case crm:building ?crmUri .
  }
} INSERT {
  GRAPH ?g {
    ?case crm:building ?building .
  }
} WHERE {
  GRAPH ?g {
    ?building a gebouw:Gebouw ;
       dct:identifier ?crmId .
    BIND(IRI(CONCAT("http://data.rollvolet.be/buildings/", ?crmId)) as ?crmUri)
    ?building dct:identifier ?crmId .
    ?case a dossier:Dossier ; crm:building ?crmUri .
  }
}

;

DELETE {
  GRAPH ?g {
    ?tel vcard:hasTelephone ?crmUri .
  }
} INSERT {
  GRAPH ?g {
    ?building vcard:hasTelephone ?tel .
  }
} WHERE {
  GRAPH ?g {
    ?tel a vcard:Telephone ; vcard:hasTelephone ?crmUri .
    FILTER(STRSTARTS(STR(?crmUri), "http://data.rollvolet.be/buildings/"))
    BIND(STRAFTER(STR(?crmUri), "http://data.rollvolet.be/buildings/") as ?crmId)
    ?building a gebouw:Gebouw ;
       dct:identifier ?crmId .
  }
}

;

DELETE {
  GRAPH ?g {
    ?building crm:parentId ?number .
  }
} INSERT {
  GRAPH ?g {
    ?customer schema:affiliation ?building .
  }
} WHERE {
  GRAPH ?g {
    ?customer a vcard:VCard ;
       vcard:hasUID ?number .
    ?building a gebouw:Gebouw ; crm:parentId ?number .
  }
}

;

DELETE {
  GRAPH ?g {
    ?email vcard:hasEmail ?crmUri .
  }
} INSERT {
  GRAPH ?g {
    ?building vcard:hasEmail ?email .
  }
} WHERE {
  GRAPH ?g {
    ?email a vcard:Email ; vcard:hasEmail ?crmUri .
    FILTER(STRSTARTS(STR(?crmUri), "http://data.rollvolet.be/buildings/"))
    BIND(STRAFTER(STR(?crmUri), "http://data.rollvolet.be/buildings/") as ?crmId)
    ?building a gebouw:Gebouw ;
       dct:identifier ?crmId .
  }
}

  }

  write_query("#{timestamp}-remove-old-buildings-links-sensitive", q)
end
