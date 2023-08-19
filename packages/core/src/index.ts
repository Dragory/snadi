function isPromise(input: any): input is Promise<any> {
  return input instanceof Promise;
}

export type Mappable = {
  toEntity: (raw: any) => any;
};

export type MappableInputType<T extends Mappable> = Parameters<T["toEntity"]>[0];
export type MappableOutputType<T extends Mappable> = Awaited<ReturnType<T["toEntity"]>>;

export type BaseRelationship<
  LocalEntityDef extends EntityDefinition,
  OtherEntityDef extends EntityDefinition,
> = {
  otherEntity: OtherEntityDef;
  load: (entities: Array<MappableOutputType<LocalEntityDef>>) => AsyncIterable<MappableInputType<OtherEntityDef>> | Promise<Iterable<MappableInputType<OtherEntityDef>>>;
};

export type ManyRelationship<
  LocalEntityDef extends EntityDefinition,
  OtherEntityDef extends EntityDefinition
> = BaseRelationship<LocalEntityDef, OtherEntityDef> & {
  attach: (otherEntities: Array<MappableOutputType<OtherEntityDef>>) => (entity: MappableOutputType<LocalEntityDef>) => Array<MappableOutputType<OtherEntityDef>>;
};

export type OneRelationship<
  LocalEntityDef extends EntityDefinition,
  OtherEntityDef extends EntityDefinition
> = BaseRelationship<LocalEntityDef, OtherEntityDef> & {
  attach: (otherEntities: Array<MappableOutputType<OtherEntityDef>>) => (entity: MappableOutputType<LocalEntityDef>) => MappableOutputType<OtherEntityDef> | null;
};

export type EntityDefinition = Mappable;

export type RelationsToLoad = {
  [key: string]:
    | ManyRelationship<EntityDefinition, EntityDefinition>
    | [ManyRelationship<EntityDefinition, EntityDefinition>, RelationsToLoad]
    | OneRelationship<EntityDefinition, EntityDefinition>
    | [OneRelationship<EntityDefinition, EntityDefinition>, RelationsToLoad];
};

export type WithLoadedRelations<Entity, Relations extends RelationsToLoad | undefined> = Entity & {
  [K in keyof Relations]:
    Relations[K] extends [ManyRelationship<any, any>, RelationsToLoad]
      // [ManyRelationship, nested relations]
      ? Array<MappableOutputType<Relations[K][0]["otherEntity"]> & WithLoadedRelations<MappableOutputType<Relations[K][0]["otherEntity"]>, Relations[K][1]>>
      // Just ManyRelationship
      : Relations[K] extends ManyRelationship<any, any>
        ? Array<MappableOutputType<Relations[K]["otherEntity"]>>
        // [OneRelationship, nested relations]
        : Relations[K] extends [OneRelationship<any, any>, RelationsToLoad]
          ? (MappableOutputType<Relations[K][0]["otherEntity"]> & WithLoadedRelations<MappableOutputType<Relations[K][0]["otherEntity"]>, Relations[K][1]>) | null
          // Just OneRelationship
          : Relations[K] extends OneRelationship<any, any>
            ? MappableOutputType<Relations[K]["otherEntity"]> | null
            : never;
};

export function mapToEntity<EntityDef extends EntityDefinition>(entityDef: EntityDef, data: MappableInputType<EntityDef>) {
  return entityDef.toEntity(data);
}

export async function mapArrayToEntity<EntityDef extends EntityDefinition>(entityDef: EntityDef, arr: AsyncIterable<MappableInputType<EntityDef>> | Iterable<MappableInputType<EntityDef>>) {
  const result: Array<MappableOutputType<EntityDef>> = [];
  for await (const data of arr) {
      result.push(await entityDef.toEntity(data));
  }
  return result;
}

export async function loadRelationsForArray<Entity, Relations extends RelationsToLoad>(
  entities: Entity[],
  relations: Relations
): Promise<Array<WithLoadedRelations<Entity, Relations>>> {
  await Promise.all(Object.entries(relations).map(async ([key, relationshipOrNested]) => {
      const relationship = Array.isArray(relationshipOrNested) ? relationshipOrNested[0] : relationshipOrNested;
      const subrelations = Array.isArray(relationshipOrNested) ? relationshipOrNested[1] : {};

      const loadedEntities: any[] = [];
      const rawIterable = relationship.load(entities);
      const iterable = isPromise(rawIterable) ? await rawIterable : rawIterable;
      for await (const data of iterable) {
          loadedEntities.push(await relationship.otherEntity.toEntity(data));
      }
      const loadedEntitiesWithNestedRelations = Object.keys(subrelations).length
          ? await loadRelationsForArray(loadedEntities, subrelations)
          : loadedEntities;
      const attachFn = relationship.attach(loadedEntitiesWithNestedRelations);
      for (const entity of entities) {
          (entity as WithLoadedRelations<Entity, Relations>)[key as keyof WithLoadedRelations<Entity, Relations>] = attachFn(entity);
      }
  }));

  return entities as Array<WithLoadedRelations<Entity, Relations>>;
}

export async function loadRelationsForEntity<Entity, Relations extends RelationsToLoad>(
  entity: Entity,
  relations: Relations
): Promise<WithLoadedRelations<Entity, Relations>> {
  return (await loadRelationsForArray([entity], relations))[0];
}
