def request_calendar_events_to_triplestore client
  timespec_regex = /^GD|^NM|^VM|^vanaf([\d:.\s]*)\s*uur|^rond([\d:.\s]*)\s*uur|^([\d:.\s]*)\s*uur\s(\(stipt\))?|^([\d:.\s]*)-([^\s]*)/

  graph = RDF::Graph.new
  timestamp = DateTime.now.strftime("%Y%m%d%H%M%S")
  visit_agenda = RDF::URI "http://data.rollvolet.be/calendars/88e92b1b-c3e2-4a2e-a7a4-d34aee6c7746"

  sql_query = "SELECT v.BezoekId, v.Bezoekdatum, v.MsObjectId, v.Opmerking, v.AfspraakOnderwerp,
a.AanvraagID, e.Initialen, c.Naam, c.ID as KlantID, v.GebouwID as GID,
c.Adres1 as KAdres1, c.Adres2 as KAdres2, c.Adres3 as KAdres3, c.Postcode as KPostcode, c.Gemeente as KGemeente,
b.Adres1 as BAdres1, b.Adres2 as BAdres2, b.Adres3 as BAdres3, b.Postcode as BPostcode, b.Gemeente as BGemeente
FROM TblBezoek v
INNER JOIN TblAanvraag a ON v.AanvraagId = a.AanvraagID
INNER JOIN tblData c ON c.ID = v.KlantID AND c.DataType = 'KLA'
LEFT JOIN tblData b ON b.ParentID = v.KlantID AND b.ID = v.GebouwID  AND b.DataType = 'GEB'
LEFT JOIN TblPersoneel e ON a.Bezoeker = e.Voornaam
WHERE a.Bezoek = 1 AND v.Bezoekdatum IS NOT NULL
ORDER BY v.Bezoekdatum DESC
"
  visits = client.execute(sql_query)
  count = 0
  visits.each_with_index do |visit, i|
    uuid = Mu.generate_uuid()
    visit_uri = RDF::URI(BASE_URI % { :resource => "calendar-events", :id => uuid })
    visit_date = visit["Bezoekdatum"].to_date
    request_id = visit["AanvraagID"]
    timespec_match = timespec_regex.match visit["AfspraakOnderwerp"]
    timespec = timespec_match[0] unless timespec_match.nil?
    request_reference = "AD#{format_request_number(request_id)} #{visit["Initialen"]}".strip
    subject = [timespec, visit["Naam"], request_reference, visit["Opmerking"]]
                .filter { |b| !b.nil? }
                .join(" | ")
    application_url = RDF::URI("https://rkb.rollvolet.be/case/#{visit["KlantID"]}/request/#{request_id}")
    source = if visit["MsObjectId"] then "RKB" else "Access" end
    request_uri = RDF::URI(BASE_URI % { :resource => "requests", :id => request_id })
    if visit["GID"]
      street = [visit["BAdres1"], visit["BAdres2"], visit["BAdres3"]].filter { |a| !a.nil? }.join(' ').strip
      city = [visit["BPostcode"], visit["BGemeente"]].filter { |a| !a.nil? }.join(' ').strip
    else
      street = [visit["KAdres1"], visit["KAdres2"], visit["KAdres3"]].filter { |a| !a.nil? }.join(' ').strip
      city = [visit["KPostcode"], visit["KGemeente"]].filter { |a| !a.nil? }.join(' ').strip
    end
    address_lines = [street, city].filter { |a| !a.nil? and !a.empty? }
    address = address_lines.join(', ').strip if address_lines.length > 0

    graph << RDF.Statement(visit_uri, RDF.type, NCAL.Event)
    graph << RDF.Statement(visit_uri, MU_CORE.uuid, uuid)
    graph << RDF.Statement(visit_uri, NCAL.uid, visit["MsObjectId"]) if visit["MsObjectId"]
    graph << RDF.Statement(visit_uri, NCAL.date, visit_date)
    graph << RDF.Statement(visit_uri, NCAL.summary, subject)
    graph << RDF.Statement(visit_uri, DCT.subject, request_uri)
    graph << RDF.Statement(visit_uri, NCAL.url, application_url)
    graph << RDF.Statement(visit_uri, NCAL.location, address) if address
    graph << RDF.Statement(visit_uri, DCT.source, source)
    graph << RDF.Statement(visit_uri, DCT.identifier, visit["BezoekId"].to_s)
    graph << RDF.Statement(visit_agenda, NCAL.component, visit_uri)

    # Legacy IDs useful for future conversions
    graph << RDF.Statement(request_uri, DCT.identifier, request_id.to_s)

    if ((i + 1) % 1000 == 0)
      Mu.log.info "Processed #{i + 1} records. Will write to file"
      write_graph("#{timestamp}-request-calendar-events-#{i + 1}-sensitive", graph)
      graph = RDF::Graph.new
    end

    count = i
  end

  # Writing last iteration to file
  write_graph("#{timestamp}-request-calendar-events-#{count + 1}-sensitive", graph)

  Mu.log.info "Generated #{count + 1} calendar-events for requests"
end

def order_calendar_events_to_triplestore client
  graph = RDF::Graph.new
  timestamp = DateTime.now.strftime("%Y%m%d%H%M%S")
  planning_agenda = RDF::URI("http://data.rollvolet.be/calendars/0147d534-d7c2-49dc-bd8f-bb6951664017")

  sql_query = "SELECT o.VastgelegdeDatum, o.PlanningMsObjectId, o.Opmerking, o.AanvraagId,
o.UrenGepland, o.ManGepland, c.Naam, o.OfferteID,
c.ID as KlantID, o.GebouwID as GID,
c.Adres1 as KAdres1, c.Adres2 as KAdres2, c.Adres3 as KAdres3, c.Postcode as KPostcode, c.Gemeente as KGemeente,
b.Adres1 as BAdres1, b.Adres2 as BAdres2, b.Adres3 as BAdres3, b.Postcode as BPostcode, b.Gemeente as BGemeente,
(
  SELECT e.Initialen
  FROM TblPersoneel e
  WHERE r.Bezoeker = e.Voornaam
) as visitor,
(
    SELECT STRING_AGG(t.Voornaam, ', ') WITHIN GROUP (ORDER BY t.Voornaam ASC)
    FROM TblOrderTechnician ot
    LEFT JOIN TblPersoneel t ON ot.EmployeeId = t.PersoneelId
    WHERE ot.OrderId = o.OfferteID
    GROUP BY ot.OrderId
) as technicians
FROM tblOfferte o
INNER JOIN tblData c ON c.ID = o.KlantID  AND c.DataType = 'KLA'
LEFT JOIN tblData b ON b.ParentID = o.KlantID  AND b.ID = o.GebouwID  AND b.DataType = 'GEB'
LEFT JOIN TblAanvraag r ON r.AanvraagID  = o.AanvraagId
WHERE o.MuntBestel  = 'EUR' AND o.VastgelegdeDatum IS NOT NULL
ORDER BY o.VastgelegdeDatum DESC"

  visits = client.execute(sql_query)
  count = 0
  visits.each_with_index do |visit, i|
    uuid = Mu.generate_uuid()
    visit_uri = RDF::URI(BASE_URI % { :resource => "calendar-events", :id => uuid })
    visit_date = Date.parse visit["VastgelegdeDatum"]
    request_id = visit["AanvraagId"]
    request_reference = "AD#{format_request_number(request_id)} #{visit["visitor"]}".strip
    order_id = visit["OfferteID"]
    timespec = "GD" # no timespec available in current data
    nb_of_persons = if visit["ManGepland"] then visit["ManGepland"].to_i else 0 end
    nb_of_hours = if visit["UrenGepland"] then visit["UrenGepland"].to_i else 0 end
    workload = "#{nb_of_persons}p x #{nb_of_hours}u #{visit["technicians"]}".strip
    subject = [timespec, visit["Naam"], request_reference, workload]
                .filter { |b| !b.nil? }
                .join(" | ")
    application_url = RDF::URI("https://rkb.rollvolet.be/case/#{visit["KlantID"]}/order/#{order_id}")
    source = if visit["PlanningMsObjectId"] then "RKB" else "Access" end
    order_uri = RDF::URI(BASE_URI % { :resource => "orders", :id => order_id })
    if visit["GID"]
      street = [visit["BAdres1"], visit["BAdres2"], visit["BAdres3"]].filter { |a| !a.nil? }.join(' ').strip
      city = [visit["BPostcode"], visit["BGemeente"]].filter { |a| !a.nil? }.join(' ').strip
    else
      street = [visit["KAdres1"], visit["KAdres2"], visit["KAdres3"]].filter { |a| !a.nil? }.join(' ').strip
      city = [visit["KPostcode"], visit["KGemeente"]].filter { |a| !a.nil? }.join(' ').strip
    end
    address_lines = [street, city].filter { |a| !a.nil? and !a.empty? }
    address = address_lines.join(', ').strip if address_lines.length > 0

    graph << RDF.Statement(visit_uri, RDF.type, NCAL.Event)
    graph << RDF.Statement(visit_uri, MU_CORE.uuid, uuid)
    graph << RDF.Statement(visit_uri, NCAL.uid, visit["PlanningMsObjectId"]) if visit["PlanningMsObjectId"]
    graph << RDF.Statement(visit_uri, NCAL.date, visit_date)
    graph << RDF.Statement(visit_uri, NCAL.summary, subject)
    graph << RDF.Statement(visit_uri, NCAL.description, visit["Opmerking"]) if visit["Opmerking"]
    graph << RDF.Statement(visit_uri, DCT.subject, order_uri)
    graph << RDF.Statement(visit_uri, NCAL.url, application_url)
    graph << RDF.Statement(visit_uri, NCAL.location, address) if address
    graph << RDF.Statement(visit_uri, DCT.source, source)
    graph << RDF.Statement(visit_uri, DCT.identifier, order_id.to_s)
    graph << RDF.Statement(planning_agenda, NCAL.component, visit_uri)

    # Legacy IDs useful for future conversions
    graph << RDF.Statement(order_uri, DCT.identifier, order_id.to_s)

    if ((i + 1) % 1000 == 0)
      Mu.log.info "Processed #{i + 1} records. Will write to file"
      write_graph("#{timestamp}-order-calendar-events-#{i + 1}-sensitive", graph)
      graph = RDF::Graph.new
    end

    count = i
  end

  # Writing last iteration to file
  write_graph("#{timestamp}-order-calendar-events-#{count + 1}-sensitive", graph)

  Mu.log.info "Generated #{count + 1} calendar-events for orders"
end


def intervention_calendar_events_to_triplestore client
  timespec_regex = /^GD|^NM|^VM|^vanaf([\d:.\s]*)\s*uur|^rond([\d:.\s]*)\s*uur|^([\d:.\s]*)\s*uur\s(\(stipt\))?|^([\d:.\s]*)-([^\s]*)/

  graph = RDF::Graph.new
  timestamp = DateTime.now.strftime("%Y%m%d%H%M%S")
  planning_agenda = RDF::URI("http://data.rollvolet.be/calendars/0147d534-d7c2-49dc-bd8f-bb6951664017")

  sql_query = "SELECT v.Id, v.[Date], v.MsObjectId, v.Subject, c.Naam,
i.Id as InterventionId, i.Description, i.NbOfPersons,
c.ID as KlantID, i.BuildingId as GID,
c.Adres1 as KAdres1, c.Adres2 as KAdres2, c.Adres3 as KAdres3, c.Postcode as KPostcode, c.Gemeente as KGemeente,
b.Adres1 as BAdres1, b.Adres2 as BAdres2, b.Adres3 as BAdres3, b.Postcode as BPostcode, b.Gemeente as BGemeente,
(
    SELECT STRING_AGG(t.Voornaam, ', ') WITHIN GROUP (ORDER BY t.Voornaam ASC)
    FROM TblInterventionTechnician ot
    LEFT JOIN TblPersoneel t ON ot.EmployeeId = t.PersoneelId
    WHERE ot.InterventionId  = i.Id
    GROUP BY ot.InterventionId
) as technicians
FROM TblIntervention i
INNER JOIN TblPlanningEvent v ON i.Id = v.InterventionId
INNER JOIN tblData c ON c.ID = i.CustomerId AND c.DataType = 'KLA'
LEFT JOIN tblData b ON b.ParentID = i.CustomerId  AND b.ID = i.BuildingId AND b.DataType = 'GEB'
WHERE v.[Date] IS NOT NULL
"
  visits = client.execute(sql_query)
  count = 0
  visits.each_with_index do |visit, i|
    uuid = Mu.generate_uuid()
    visit_uri = RDF::URI(BASE_URI % { :resource => "calendar-events", :id => uuid })
    visit_date = visit["Date"].to_date
    intervention_id = visit["InterventionId"]
    if visit["Subject"]
      subject_without_prefix = visit["Subject"]["Interventie: ".length..]
      timespec_match = timespec_regex.match subject_without_prefix
      timespec = timespec_match[0] unless timespec_match.nil?
    end
    timespec = "GD" if timespec.nil?
    intervention_reference = "IR#{intervention_id}".strip
    nb_of_persons = if visit["NbOfPersons"] then visit["NbOfPersons"].to_i else 0 end
    workload = "#{nb_of_persons}p #{visit["technicians"]}".strip
    subject = [timespec, visit["Naam"], intervention_reference, workload]
                .filter { |b| !b.nil? }
                .join(" | ")
    application_url = RDF::URI("https://rkb.rollvolet.be/case/#{visit["KlantID"]}/intervention/#{intervention_id}")
    source = if visit["MsObjectId"] then "RKB" else "Access" end
    intervention_uri = RDF::URI(BASE_URI % { :resource => "interventions", :id => intervention_id })
    if visit["GID"]
      street = [visit["BAdres1"], visit["BAdres2"], visit["BAdres3"]].filter { |a| !a.nil? }.join(' ').strip
      city = [visit["BPostcode"], visit["BGemeente"]].filter { |a| !a.nil? }.join(' ').strip
    else
      street = [visit["KAdres1"], visit["KAdres2"], visit["KAdres3"]].filter { |a| !a.nil? }.join(' ').strip
      city = [visit["KPostcode"], visit["KGemeente"]].filter { |a| !a.nil? }.join(' ').strip
    end
    address_lines = [street, city].filter { |a| !a.nil? and !a.empty? }
    address = address_lines.join(', ').strip if address_lines.length > 0

    graph << RDF.Statement(visit_uri, RDF.type, NCAL.Event)
    graph << RDF.Statement(visit_uri, MU_CORE.uuid, uuid)
    graph << RDF.Statement(visit_uri, NCAL.uid, visit["MsObjectId"]) if visit["MsObjectId"]
    graph << RDF.Statement(visit_uri, NCAL.date, visit_date)
    graph << RDF.Statement(visit_uri, NCAL.summary, subject)
    graph << RDF.Statement(visit_uri, NCAL.description, visit["Description"]) if visit["Description"]
    graph << RDF.Statement(visit_uri, DCT.subject, intervention_uri)
    graph << RDF.Statement(visit_uri, NCAL.url, application_url)
    graph << RDF.Statement(visit_uri, NCAL.location, address) if address
    graph << RDF.Statement(visit_uri, DCT.source, source)
    graph << RDF.Statement(visit_uri, DCT.identifier, visit["Id"].to_s)
    graph << RDF.Statement(planning_agenda, NCAL.component, visit_uri)

    # Legacy IDs useful for future conversions
    graph << RDF.Statement(intervention_uri, DCT.identifier, intervention_id.to_s)

    if ((i + 1) % 1000 == 0)
      Mu.log.info "Processed #{i + 1} records. Will write to file"
      write_graph("#{timestamp}-intervention-calendar-events-#{i + 1}-sensitive", graph)
      graph = RDF::Graph.new
    end

    count = i
  end

  # Writing last iteration to file
  write_graph("#{timestamp}-intervention-calendar-events-#{count + 1}-sensitive", graph)

  Mu.log.info "Generated #{count + 1} calendar-events for interventions"
end
