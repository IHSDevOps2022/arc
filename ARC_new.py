import pandas as pd
import os
from datetime import datetime, timedelta
import json
import re
from collections import defaultdict, Counter
import requests
from bs4 import BeautifulSoup
import time
import feedparser
from urllib.parse import quote_plus

class MediaStoryKeywordSearcher:
    def __init__(self):
        self.media_sources = self._initialize_media_sources()
        self.experts_df = None
        self.search_results = defaultdict(list)
        self.expert_mentions = defaultdict(list)
        # Hardcoded keywords list
        self.keywords = [
            "Sexual harassment", "Sexual Assault", "Rape", "Raping", "Grope", "Groping", 
            "Pornography", "Porn", "OnlyFans", "Intoxicated", "Inebriated", "Drunk", 
            "Public intoxication", "Arrested", "Fired", "Suspended", "Murder", "Molestation", 
            "Molest", "Molesting", "DUI", "DWI", "Prostitution", "Prostitute", "Assault", 
            "Assaulting", "Protest", "Protesting", "Crime", "Criminal", "UnKoch", "Nazi", 
            "White Supremacy", "White supremacist", "White Nationalism", "White Nationalist", 
            "Resigned", "Resignation", "Academic dishonesty", "Academic fraud", "Plagiarism", 
            "Falsifying data", "Falsified", "p-hacking", "Abbeville Institute", "H.L. Mencken Club", 
            "VDARE", "Underage", "Pervert", "Perverted", "Accuse", "Accused", "Accusation", 
            "Alleged", "Allegation", "Allegations", "Antisemitic", "Antisemite", "Sexist", 
            "Sexism", "Sexual", "Racist", "Racism", "Racial", "Incest", "Incestuous", 
            "Sex trafficking", "Sex trafficker", "Misconduct", "League of the South", "KKK", 
            "Klu Klux Klan", "Disbarred", "Guilty", "Domestic violence", "Domestic abuse", 
            "Allegation", "Allegations", "Alleged", "Theft", "Fugitive", "Felony", "Embezzle", 
            "Embezzlement", "Embezzling", "Pedophile", "Pedophilia", "Offensive", "Inappropriate", 
            "Inappropriately", "Hate", "Hatred", "Richard Spencer", "Child Abuse", 
            "Council of Conservative Citizens", "Institute of Historical Review", 
            "Occidental Quarterly", "Mankind Quarterly", "Violent", "Violence", 
            "Registered Sex Offender", "Sex Offender", "Sexting", "Homophobe", "Homophobic", 
            "Conspiracy theorist", "Conspiracy theory", "Lynch", "Lynching", "MeToo", "Title IX", 
            "Die", "Death", "Dead", "Assassinate", "Assassination", "N-word", "Misogyny", 
            "Misogynistic", "Misogynist", "Drugging", "Ruffie", "Ruffies", "Roofies", "Roofie", 
            "Date-Rape", "Rohypnol", "Flunitrazepam", "Ketamine", "Meth", "Methamphetamine", 
            "Cocaine", "Heroin", "Oxycodone", "Oxy", "LSD", "Holocaust Denial", "Holocaust Denier"
        ]
        
    def _initialize_media_sources(self):
        """Initialize media sources with their search endpoints"""
        return {
            'CNN': {
                'rss': 'http://rss.cnn.com/rss/cnn_topstories.rss',
                'search_url': 'https://www.cnn.com/search?q=',
                'name': 'CNN'
            },
            'Fox News': {
                'rss': 'http://feeds.foxnews.com/foxnews/latest',
                'search_url': 'https://www.foxnews.com/search-results/search?q=',
                'name': 'Fox News'
            },
            'NBC News': {
                'rss': 'http://feeds.nbcnews.com/nbcnews/public/news',
                'search_url': 'https://www.nbcnews.com/search/?q=',
                'name': 'NBC News'
            },
            'ABC News': {
                'rss': 'https://abcnews.go.com/abcnews/topstories',
                'search_url': 'https://abcnews.go.com/search?searchtext=',
                'name': 'ABC News'
            },
            'CBS News': {
                'rss': 'https://www.cbsnews.com/latest/rss/main',
                'search_url': 'https://www.cbsnews.com/search/?q=',
                'name': 'CBS News'
            },
            'New York Times': {
                'rss': 'https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml',
                'search_url': 'https://www.nytimes.com/search?query=',
                'name': 'New York Times'
            },
            'Wall Street Journal': {
                'rss': 'https://feeds.a.dj.com/rss/RSSWorldNews.xml',
                'search_url': 'https://www.wsj.com/search?query=',
                'name': 'Wall Street Journal'
            },
            'Bloomberg': {
                'rss': 'https://www.bloomberg.com/feed/podcast/top-news.xml',
                'search_url': 'https://www.bloomberg.com/search?query=',
                'name': 'Bloomberg'
            },
            'Washington Post': {
                'rss': 'http://feeds.washingtonpost.com/rss/politics',
                'search_url': 'https://www.washingtonpost.com/search?query=',
                'name': 'Washington Post'
            },
            'MSNBC': {
                'rss': 'http://www.msnbc.com/feeds/latest',
                'search_url': 'https://www.msnbc.com/search/?q=',
                'name': 'MSNBC'
            }
        }
    
    def load_experts(self, experts_path):
        """Load experts from Excel file"""
        try:
            self.experts_df = pd.read_excel(experts_path)
            print(f"\n✓ Loaded experts data: {len(self.experts_df)} experts")
            
            # Standardize column names
            self.experts_df.columns = self.experts_df.columns.str.lower().str.strip()
            
            # Create full name column
            if 'firstname' in self.experts_df.columns and 'lastname' in self.experts_df.columns:
                self.experts_df['full_name'] = (
                    self.experts_df['firstname'].astype(str).str.strip() + ' ' + 
                    self.experts_df['lastname'].astype(str).str.strip()
                )
                self.experts_df['full_name_lower'] = self.experts_df['full_name'].str.lower()
            
            return True
        except Exception as e:
            print(f"❌ Error loading experts file: {str(e)}")
            return False
    
    def search_rss_feeds(self, days_back=7):
        """Search RSS feeds for keywords"""
        print(f"\n🔍 Searching RSS feeds for {len(self.keywords)} keywords from the last {days_back} days...")
        print(f"Keywords include: {', '.join(self.keywords[:5])}... and {len(self.keywords)-5} more")
        
        cutoff_date = datetime.now() - timedelta(days=days_back)
        total_found = 0
        
        for source_name, source_info in self.media_sources.items():
            print(f"\nSearching {source_name}...")
            try:
                feed = feedparser.parse(source_info['rss'])
                source_results = []
                
                for entry in feed.entries:
                    # Check if entry is recent enough
                    if hasattr(entry, 'published_parsed'):
                        pub_date = datetime(*entry.published_parsed[:6])
                        if pub_date < cutoff_date:
                            continue
                    
                    # Get entry content
                    title = entry.get('title', '').lower()
                    summary = entry.get('summary', '').lower()
                    content = title + ' ' + summary
                    
                    # Check for keywords
                    matching_keywords = []
                    for keyword in self.keywords:
                        if keyword.lower() in content:
                            matching_keywords.append(keyword)
                    
                    if matching_keywords:
                        result = {
                            'source': source_name,
                            'title': entry.get('title', 'No title'),
                            'url': entry.get('link', ''),
                            'published': entry.get('published', 'Unknown'),
                            'summary': entry.get('summary', '')[:200] + '...',
                            'keywords_found': matching_keywords,
                            'experts_mentioned': []
                        }
                        
                        # Check for expert mentions
                        if self.experts_df is not None:
                            for idx, expert in self.experts_df.iterrows():
                                expert_name = expert.get('full_name', '')
                                if expert_name and expert_name.lower() in content:
                                    result['experts_mentioned'].append(expert_name)
                                    self.expert_mentions[expert_name].append(result)
                        
                        source_results.append(result)
                        total_found += 1
                
                self.search_results[source_name] = source_results
                print(f"✓ Found {len(source_results)} articles with keywords")
                
            except Exception as e:
                print(f"❌ Error searching {source_name}: {str(e)}")
        
        print(f"\n✓ Total articles found: {total_found}")
        return total_found
    
    def simulate_web_search(self, days_back=7):
        """Simulate web search for keywords (in production, use actual APIs)"""
        print(f"\n🔍 Simulating web search for keywords...")
        
        # In production, this would use:
        # - Google Custom Search API
        # - Bing News Search API
        # - NewsAPI.org
        # - Individual media outlet APIs
        
        print("\n⚠️  Note: This is a simulation. In production, implement actual API calls to:")
        print("   - Google Custom Search API")
        print("   - Bing News Search API")
        print("   - NewsAPI.org")
        print("   - Individual media outlet APIs")
        
        # For now, we'll skip the simulation since it's not providing real data
        return
    
    def generate_keyword_report(self, output_path='media_keyword_analysis.md'):
        """Generate comprehensive keyword analysis report"""
        report = []
        
        # Header
        report.append("# Media Coverage Analysis - Sensitive Content Monitoring")
        report.append(f"\n*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
        report.append(f"\n**Total keywords monitored:** {len(self.keywords)}")
        
        # Executive Summary
        total_articles = sum(len(articles) for articles in self.search_results.values())
        total_expert_mentions = sum(len(mentions) for mentions in self.expert_mentions.values())
        
        report.append(f"\n## Executive Summary")
        report.append(f"- Total articles found: {total_articles}")
        report.append(f"- Media outlets searched: {len(self.media_sources)}")
        report.append(f"- Keywords tracked: {len(self.keywords)}")
        report.append(f"- Expert mentions found: {total_expert_mentions}")
        
        # Coverage by Keyword
        report.append("\n## Coverage Analysis by Keyword\n")
        keyword_stats = defaultdict(int)
        keyword_sources = defaultdict(set)
        
        for source, articles in self.search_results.items():
            for article in articles:
                for keyword in article['keywords_found']:
                    keyword_stats[keyword] += 1
                    keyword_sources[keyword].add(source)
        
        # Sort keywords by frequency
        sorted_keywords = sorted(keyword_stats.items(), key=lambda x: x[1], reverse=True)
        
        if sorted_keywords:
            report.append("### Most Frequently Found Keywords")
            for keyword, count in sorted_keywords[:20]:  # Top 20 most found
                sources = keyword_sources.get(keyword, set())
                report.append(f"\n**{keyword}**")
                report.append(f"- Articles found: {count}")
                report.append(f"- Media outlets covering: {len(sources)}")
                if sources:
                    report.append(f"- Outlets: {', '.join(sorted(sources))}")
            
            if len(sorted_keywords) > 20:
                report.append(f"\n*... and {len(sorted_keywords) - 20} more keywords with matches*")
        
        # Keywords not found
        not_found = [kw for kw in self.keywords if kw not in keyword_stats]
        if not_found:
            report.append(f"\n### Keywords Not Found in Recent Coverage ({len(not_found)} total)")
            report.append(", ".join(not_found[:20]))
            if len(not_found) > 20:
                report.append(f"... and {len(not_found) - 20} more")
        
        # Coverage by Media Outlet
        report.append("\n## Coverage by Media Outlet\n")
        for source, articles in sorted(self.search_results.items()):
            if articles:
                report.append(f"### {source}")
                report.append(f"*{len(articles)} articles found*\n")
                
                # Show first 5 articles
                for article in articles[:5]:
                    report.append(f"**{article['title']}**")
                    report.append(f"- Published: {article['published']}")
                    report.append(f"- Keywords: {', '.join(article['keywords_found'])}")
                    if article['experts_mentioned']:
                        report.append(f"- Experts mentioned: {', '.join(article['experts_mentioned'])}")
                    report.append(f"- [Link]({article['url']})")
                    report.append("")
                
                if len(articles) > 5:
                    report.append(f"*... and {len(articles) - 5} more articles*\n")
        
        # Expert Mentions
        if self.expert_mentions:
            report.append("\n## Expert Media Mentions\n")
            report.append("*Experts from your database mentioned in media coverage:*\n")
            
            for expert, mentions in sorted(self.expert_mentions.items(), 
                                         key=lambda x: len(x[1]), reverse=True):
                report.append(f"### {expert}")
                report.append(f"*Mentioned in {len(mentions)} articles*\n")
                
                for mention in mentions[:3]:
                    report.append(f"- **{mention['title']}** ({mention['source']})")
                
                if len(mentions) > 3:
                    report.append(f"- *... and {len(mentions) - 3} more mentions*")
                report.append("")
        
        # Save report
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report))
        
        print(f"\n✓ Keyword analysis report saved to: {output_path}")
        return output_path
    
    def export_search_results(self, output_prefix='media_search_results'):
        """Export search results to CSV/Excel"""
        all_results = []
        
        for source, articles in self.search_results.items():
            for article in articles:
                record = {
                    'source': source,
                    'title': article['title'],
                    'url': article['url'],
                    'published': article['published'],
                    'summary': article['summary'],
                    'keywords_found': ', '.join(article['keywords_found']),
                    'experts_mentioned': ', '.join(article['experts_mentioned']),
                    'num_keywords': len(article['keywords_found']),
                    'has_expert_mention': len(article['experts_mentioned']) > 0
                }
                all_results.append(record)
        
        if all_results:
            df = pd.DataFrame(all_results)
            df.to_csv(f'{output_prefix}.csv', index=False)
            df.to_excel(f'{output_prefix}.xlsx', index=False)
            print(f"✓ Search results exported to {output_prefix}.csv and .xlsx")
        else:
            print("❌ No results to export")
    
    def create_keyword_dashboard_data(self, output_path='keyword_dashboard_data.json'):
        """Create data for visualization dashboard"""
        dashboard_data = {
            'generated': datetime.now().isoformat(),
            'keywords': self.keywords,
            'summary': {
                'total_articles': sum(len(articles) for articles in self.search_results.values()),
                'total_sources': len([s for s, articles in self.search_results.items() if articles]),
                'total_expert_mentions': sum(len(mentions) for mentions in self.expert_mentions.values())
            },
            'keyword_metrics': {},
            'source_metrics': {},
            'timeline': defaultdict(lambda: defaultdict(int)),
            'expert_visibility': {}
        }
        
        # Calculate metrics
        for source, articles in self.search_results.items():
            for article in articles:
                # Keyword metrics
                for keyword in article['keywords_found']:
                    if keyword not in dashboard_data['keyword_metrics']:
                        dashboard_data['keyword_metrics'][keyword] = {
                            'count': 0,
                            'sources': set()
                        }
                    dashboard_data['keyword_metrics'][keyword]['count'] += 1
                    dashboard_data['keyword_metrics'][keyword]['sources'].add(source)
                
                # Source metrics
                if source not in dashboard_data['source_metrics']:
                    dashboard_data['source_metrics'][source] = {
                        'total_articles': 0,
                        'keywords_covered': set()
                    }
                dashboard_data['source_metrics'][source]['total_articles'] += 1
                dashboard_data['source_metrics'][source]['keywords_covered'].update(article['keywords_found'])
                
                # Timeline data
                try:
                    pub_date = article['published'][:10]  # Get YYYY-MM-DD
                    for keyword in article['keywords_found']:
                        dashboard_data['timeline'][pub_date][keyword] += 1
                except:
                    pass
        
        # Expert visibility
        for expert, mentions in self.expert_mentions.items():
            dashboard_data['expert_visibility'][expert] = {
                'mention_count': len(mentions),
                'sources': list(set(m['source'] for m in mentions))
            }
        
        # Convert sets to lists for JSON serialization
        for keyword in dashboard_data['keyword_metrics']:
            dashboard_data['keyword_metrics'][keyword]['sources'] = list(
                dashboard_data['keyword_metrics'][keyword]['sources']
            )
        for source in dashboard_data['source_metrics']:
            dashboard_data['source_metrics'][source]['keywords_covered'] = list(
                dashboard_data['source_metrics'][source]['keywords_covered']
            )
        
        # Save dashboard data
        with open(output_path, 'w') as f:
            json.dump(dashboard_data, f, indent=2, default=str)
        
        print(f"✓ Dashboard data saved to {output_path}")
        return dashboard_data


# Main execution
if __name__ == "__main__":
    print("=== Media Story Keyword Search & Analysis System ===")
    print("Searching for sensitive content across media outlets\n")
    
    # Create searcher
    searcher = MediaStoryKeywordSearcher()
    
    print(f"Monitoring {len(searcher.keywords)} keywords for sensitive content")
    
    # Load experts (optional)
    load_experts = input("\nLoad experts database? (y/n) [y]: ").strip().lower()
    if load_experts != 'n':
        experts_path = input("Enter path to Experts.xlsx [/users/jronyak/desktop/Experts.xlsx]: ").strip()
        if not experts_path:
            experts_path = "/users/jronyak/desktop/Experts.xlsx"
        searcher.load_experts(experts_path)
    
    # Search time range
    days_input = input("\nSearch articles from last N days [7]: ").strip()
    days_back = int(days_input) if days_input else 7
    
    # Perform searches
    print("\n📡 Starting media search...")
    
    # Search RSS feeds
    searcher.search_rss_feeds(days_back=days_back)
    
    # Note about web search
    print("\n⚠️  Web search simulation skipped. Implement actual API integrations for production use.")
    
    # Generate reports
    print("\n📊 Generating analysis reports...")
    searcher.generate_keyword_report()
    searcher.export_search_results()
    searcher.create_keyword_dashboard_data()
    
    print("\n✅ Analysis complete!")
    print("Generated files:")
    print("- media_keyword_analysis.md - Comprehensive keyword analysis")
    print("- media_search_results.csv/xlsx - All search results data")
    print("- keyword_dashboard_data.json - Data for visualization dashboard")
