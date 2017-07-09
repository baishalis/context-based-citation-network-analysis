1. Run the filtering tasks on the raw dataset. <br />
2. Run FieldTagging.java on the papers left out after automated field tagging. <br />
3. Run PaperDOIKeywordsEntry.java. Input : taggedFilteredMetadata.java , Output : PaperBagofPhrases. <br />
4. Run UniqueDOICitationContext.java. Input : filteredCitations.txt , Output : DOIMergedContexts. <br />
5. Run UniqueKeyphrasesCount.java. Input : PaperBagofPhrases, Output: GlobalKeyphrasesList. <br />
6. Run ListKeywordsFromContext.java. Input : DOIMergedContexts & GlobalKeyphrasesList  , Output: DOITerms. <br />
7. Run UnionKeyphrasesContextEntry.java. Input : DOITerms & PaperBagofPhrases , Output : PaperBagofPhrases. <br />
8. Run CitationNetworkDirect.java. Input: filteredCitations.txt , Output : CitationNetworksFinal. <br />
9. Run CitationNetworkIndirect.java. Input: CitationNetworksFinal & PaperBagofPhrases , Output : CitationNetworksFinal. <br />
10. Run PaperDOICommunityEntry.java. Input : taggedfilteredMetadata.txt , Output : PaperNagofPhrases. <br />
11. Run CreateCommunityGraph.java. Input : CitationNetworksFinal & PaperBagofPhrases , Output : CommunityNetworkDirect. <br />
12. Run CreateContextCommunityGraph.java. Input : CitationNetworksFinal & PaperBagofPhrases , Output : ContextBasedCommunityNetwork. <br />
13. Run CommunityCount.java. Input : PaperBagofPhrases ,Output : CommunityAnalysisFinal <br />
14. Run MetricsExpansionDirect.java and MetricsExpansionIndirect.java. <br />
15. Run MetricsCutRatioDirect.java and MetricsCutRatioIndirect.java. <br />
16. Run MetricConductance.java and MetricConductanceIndirect.java. <br />
17. Run PaperMetricIndegreeOutdegreeDirect/Indirect.java and PaperMetricIndegreeOutdegreePatch.java(change the context.write entries in the reducer task) for both the networks after each task. <br />
18. Run MetricMedian.java. (change the column family of in the map task) <br />
19. Run MetricFOMDDirect/Indirect.java. <br />
20. Run MetricsInwardness.java (update column family to run for direct and indirect networks). <br />
