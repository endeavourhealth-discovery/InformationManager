import {Component, ElementRef, OnInit, ViewChild} from '@angular/core';
import {ConceptService} from '../../concept/concept.service';
import {FlatTreeControl} from '@angular/cdk/tree';
import {ConceptTreeNode} from '../../models/ConceptTreeNode';
import {DynamicDataSource} from '../../concept/child-hierarchy-data-source';
import {SearchResult} from '../../models/SearchResult';
import {fromEvent} from 'rxjs';
import {debounceTime, distinctUntilChanged} from 'rxjs/operators';
import {Concept} from '../../models/Concept';
import {LoggerService} from 'dds-angular8/logger';

@Component({
  selector: 'app-record-structure-library',
  templateUrl: './record-structure-library.component.html',
  styleUrls: ['./record-structure-library.component.scss']
})
export class RecordStructureLibraryComponent implements OnInit {
  @ViewChild('searchInput', {static: true}) searchInput: ElementRef;

  hasChild = (_: number, node: ConceptTreeNode) => (node.children == null) || node.children.length > 0;

  treeControl: FlatTreeControl<ConceptTreeNode>;
  treeSource: DynamicDataSource;
  searchTerm: string;
  searchResult: SearchResult;
  selectedIri: string;
  concept: Concept;

  constructor(private conceptService: ConceptService,
              private log: LoggerService) {
    this.treeControl = new FlatTreeControl<ConceptTreeNode>(
      (node: ConceptTreeNode) => node.level,
      (node: ConceptTreeNode) => true
    );
    this.treeSource = new DynamicDataSource(this.treeControl, conceptService, log);
  }

  ngOnInit() {
    this.getRoot();

    fromEvent(this.searchInput.nativeElement, 'keyup')
      .pipe(
        debounceTime(500),
        distinctUntilChanged()
      )
      .subscribe((event: any) => {
          this.search(event.target.value);
        }
      );
  }

  getRoot() {
/*    this.conceptService.getChildren(':DM_DataModel').subscribe(
      (result) => this.treeSource.data = result.map(c => ConceptTreeNode.from(c)),
      (error) => this.log.error(error)
    );*/
  }

  search(term: string) {
    this.conceptService.search({term: term, supertypes: ['cm:EntityClass']}).subscribe(
      (result) => this.searchResult = result,
      (error) => this.log.error(error)
    );
  }

  clear() {
    this.searchTerm = '';
    this.getRoot();
  }

  autoDisplay(option: Concept) {
    return option ? option.name : undefined;
  }

  navigateTree(conceptId: any) {
    console.log(conceptId);
    this.selectedIri = conceptId;
  }

  promptCreate() {

  }

}
